import re
import sqlite3
from pathlib import Path

from config import DB_FILENAME

# ── Seed skills (used only to initialise the DB on first run) ─────────────────

SKILLS_SEED: dict[str, list[str]] = {
    "Languages": [
        "python",
        "go",
        "golang",
        "rust",
        "java",
        "kotlin",
        "scala",
        "typescript",
        "javascript",
        "c++",
        "c#",
        "ruby",
        "php",
        "elixir",
        "bash",
        "sql",
        "c language",
    ],
    "Python Frameworks": [
        "fastapi",
        "django",
        "flask",
        "aiohttp",
        "starlette",
        "tornado",
        "litestar",
        "sanic",
        "django rest framework",
        "drf",
        "nest.js",
        "django ninja",
    ],
    "Python Libraries": [
        "sqlalchemy",
        "pydantic",
        "alembic",
        "celery",
        "pytest",
        "httpx",
        "aiofiles",
        "asyncpg",
        "psycopg",
        "boto3",
        "pandas",
        "numpy",
        "pydantic.v2",
        "typer",
        "click",
        "poetry",
        "uv",
        "pillow",
        "duckdb",
        "dlt",
        "delta-rs",
        "pyproject.toml",
    ],
    "Databases — Relational": [
        "postgresql",
        "postgres",
        "mysql",
        "mariadb",
        "sqlite",
        "aurora",
        "cockroachdb",
        "tidb",
        "orm",
    ],
    "Databases — NoSQL / Search": [
        "mongodb",
        "redis",
        "elasticsearch",
        "opensearch",
        "cassandra",
        "dynamodb",
        "firestore",
        "couchdb",
        "neo4j",
        "memcached",
        "vector database",
        "vector store",
    ],
    "Databases — Analytical": [
        "clickhouse",
        "bigquery",
        "snowflake",
        "redshift",
        "databricks",
        "dbt",
        "iceberg",
        "duckdb",
        "data lake",
    ],
    "Cloud": [
        "aws",
        "gcp",
        "google cloud",
        "azure",
        "lambda",
        "ec2",
        "ecs",
        "fargate",
        "s3",
        "cloud run",
        "app engine",
        "azure functions",
        "step functions",
        "bedrock",
        "api gateway",
        "cloudwatch",
        "x-ray",
    ],
    "Containers & Orchestration": [
        "kubernetes",
        "k8s",
        "docker",
        "helm",
        "argo",
        "istio",
        "envoy",
        "containerd",
        "eks",
    ],
    "IaC & CI/CD": [
        "terraform",
        "ansible",
        "pulumi",
        "cdk",
        "ci/cd",
        "github actions",
        "gitlab ci",
        "jenkins",
        "circleci",
        "argocd",
        "flux",
        "dagger",
    ],
    "Messaging & Streaming": [
        "kafka",
        "rabbitmq",
        "sqs",
        "sns",
        "pubsub",
        "nats",
        "activemq",
        "kinesis",
        "eventbridge",
    ],
    "API & Architecture": [
        "rest",
        "restful",
        "graphql",
        "grpc",
        "websocket",
        "openapi",
        "swagger",
        "proto",
        "protobuf",
        "microservices",
        "monolith",
        "serverless",
        "event.driven",
        "event sourcing",
        "cqrs",
        "domain.driven",
        "ddd",
        "hexagonal",
        "clean architecture",
        "solid",
        "dry",
        "iam",
        "auth",
    ],
    "Auth & Security": [
        "oauth",
        "oauth2",
        "openid",
        "oidc",
        "jwt",
        "saml",
        "keycloak",
        "auth0",
        "okta",
        "ssl",
        "tls",
        "vault",
        "rbac",
    ],
    "Monitoring & Observability": [
        "prometheus",
        "grafana",
        "datadog",
        "newrelic",
        "sentry",
        "opentelemetry",
        "jaeger",
        "zipkin",
        "elk",
        "loki",
        "splunk",
        "cloudwatch",
    ],
    "Testing": [
        "pytest",
        "unittest",
        "tdd",
        "bdd",
        "integration test",
        "unit test",
        "e2e",
        "testcontainers",
        "hypothesis",
        "coverage.py",
        "gcov",
    ],
    "AI / ML (mentioned in JD)": [
        "ai",
        "llm",
        "openai",
        "langchain",
        "langgraph",
        "llamaindex",
        "vector database",
        "pgvector",
        "pinecone",
        "weaviate",
        "rag",
        "fine.tun",
        "machine learning",
        "ml",
        "generative ai",
        "agentic",
        "multi-agent",
        "prompt engineering",
        "ai orchestration",
        "claude",
        "roo code",
        "cursor",
        "mcp",
    ],
    "Soft / Process": [
        "agile",
        "scrum",
        "kanban",
        "code review",
        "mentoring",
        "team lead",
        "tech lead",
        "staff engineer",
        "cross.functional",
        "stakeholder",
        "ownership",
    ],
    "Languages (non-technical)": [
        "english",
        "german",
        "deutsch",
        "french",
        "spanish",
        "dutch",
        "portuguese",
    ],
}

# Initial alias seeds: (skill_term, alias_text, language, alias_type)
# Only seeded when skill_aliases table is empty.
# More aliases can be added via SQL (see module docstring).
ALIAS_SEED: list[tuple[str, str, str, str]] = [
    ("python", "python3", "en", "variant"),
    ("python", "python 3", "en", "variant"),
]

_VALID_DB_TABLES: frozenset[str] = frozenset(
    {
        "categories",
        "skills",
        "llm_results",
        "skill_candidates",
        "skill_aliases",
    }
)

# Old-style LLM category names used before the strict-prompt change.
# Used only in _migrate_schema to remap existing llm_results / skill_candidates rows.
_OLD_CATEGORY_REMAP: dict[str, str] = {
    "languages": "Languages",
    "frameworks": "Python Frameworks",
    "libraries": "Python Libraries",
    "cloud_services": "Cloud",
    "devops": "IaC & CI/CD",
    "tools": "Containers & Orchestration",
    "concepts": "API & Architecture",
    "databases": "Databases — Relational",
}


def open_db(data_dir: Path) -> sqlite3.Connection:
    """Open (or create) the SQLite skills database in data_dir."""
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(data_dir / DB_FILENAME)
    conn.row_factory = sqlite3.Row
    return conn


def _table_is_empty(conn: sqlite3.Connection, table_name: str) -> bool:
    """Return True if the given table has no rows."""
    if table_name not in _VALID_DB_TABLES:
        raise ValueError(f"Unknown table: {table_name!r}")
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] == 0


def normalize_term(term: str) -> str:
    """Lowercase and collapse whitespace. Plain text, no regex escaping.

    Stored directly in skills.term and skill_candidates.term.
    """
    normalised = term.strip().lower()
    return re.sub(r"\s+", " ", normalised)


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables; seed categories, skills, and aliases on first run."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS categories (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS skills (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL REFERENCES categories(id),
            term        TEXT NOT NULL,
            UNIQUE(category_id, term)
        );

        CREATE TABLE IF NOT EXISTS llm_results (
            url_key     TEXT NOT NULL,
            url         TEXT NOT NULL,
            category_id INTEGER NOT NULL REFERENCES categories(id),
            skill       TEXT NOT NULL,
            is_matched  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (url_key, category_id, skill)
        );

        -- Review queue: LLM-discovered terms pending skills inclusion
        CREATE TABLE IF NOT EXISTS skill_candidates (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            term            TEXT NOT NULL,
            category_id     INTEGER NOT NULL REFERENCES categories(id),
            llm_category_id INTEGER NOT NULL REFERENCES categories(id),
            jobs_count      INTEGER DEFAULT 0,
            status          TEXT DEFAULT 'pending',
            added_date      TEXT NOT NULL,
            decided_date    TEXT,
            UNIQUE(term, category_id)
        );

        -- Aliases and multilingual synonyms for existing skills
        CREATE TABLE IF NOT EXISTS skill_aliases (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_id     INTEGER NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
            alias        TEXT NOT NULL,
            canonical    TEXT NOT NULL,
            lang         TEXT DEFAULT 'en',
            alias_type   TEXT DEFAULT 'variant',
            UNIQUE(canonical, skill_id)
        );

        CREATE INDEX IF NOT EXISTS idx_aliases_canonical ON skill_aliases(canonical);
    """)
    conn.commit()

    _migrate_schema(conn)

    if _table_is_empty(conn, "categories"):
        conn.executemany(
            "INSERT OR IGNORE INTO categories(name) VALUES(?)",
            [(name,) for name in SKILLS_SEED],
        )
        conn.commit()

    if _table_is_empty(conn, "skills"):
        seed_rows = []
        for category, terms in SKILLS_SEED.items():
            cat_row = conn.execute(
                "SELECT id FROM categories WHERE name=?", (category,)
            ).fetchone()
            if cat_row:
                for term in terms:
                    seed_rows.append((cat_row["id"], normalize_term(term)))
        conn.executemany(
            "INSERT OR IGNORE INTO skills(category_id, term) VALUES(?, ?)", seed_rows
        )
        conn.commit()
        print(f"  [DB] Seeded skills with {len(seed_rows)} terms.")

    if _table_is_empty(conn, "skill_aliases"):
        for skill_term, alias_text, lang, alias_type in ALIAS_SEED:
            row = conn.execute(
                "SELECT id FROM skills WHERE term=?", (normalize_term(skill_term),)
            ).fetchone()
            if row:
                canonical = normalize_term(alias_text)
                conn.execute(
                    """INSERT OR IGNORE INTO skill_aliases
                       (skill_id, alias, canonical, lang, alias_type)
                       VALUES (?,?,?,?,?)""",
                    (row["id"], alias_text, canonical, lang, alias_type),
                )
        conn.commit()


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Idempotent migration: convert old text-category schema to category_id FK schema.

    Detects whether migration is needed by checking for the legacy 'category' column
    on the skills table. If found, recreates all three tables with FK columns.
    Also remaps old-style LLM category names to catalog names beforehand.
    Renames legacy 'c' skill to 'c language'.
    """
    skills_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(skills)").fetchall()
    }
    if "category_id" in skills_cols:
        return  # Already migrated

    # Remap old-style LLM category names in legacy text columns
    for old, new in _OLD_CATEGORY_REMAP.items():
        conn.execute("UPDATE llm_results SET category=? WHERE category=?", (new, old))
        conn.execute(
            "UPDATE skill_candidates SET category=? WHERE category=?", (new, old)
        )
        conn.execute(
            "UPDATE skill_candidates SET llm_category=? WHERE llm_category=?",
            (new, old),
        )
    # Rename legacy 'c' term to 'c language'
    conn.execute("UPDATE skills SET term='c language' WHERE term='c'")
    conn.commit()

    # Populate categories from existing skills rows
    conn.execute(
        "INSERT OR IGNORE INTO categories(name) SELECT DISTINCT category FROM skills"
    )
    conn.execute(
        "INSERT OR IGNORE INTO categories(name) "
        "SELECT DISTINCT category FROM llm_results"
    )
    conn.execute(
        "INSERT OR IGNORE INTO categories(name) "
        "SELECT DISTINCT category FROM skill_candidates"
    )
    conn.execute(
        "INSERT OR IGNORE INTO categories(name) "
        "SELECT DISTINCT llm_category FROM skill_candidates"
    )
    conn.commit()

    # Recreate skills with category_id
    conn.executescript("""
        CREATE TABLE skills_new (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL REFERENCES categories(id),
            term        TEXT NOT NULL,
            UNIQUE(category_id, term)
        );
        INSERT INTO skills_new(id, category_id, term)
            SELECT s.id, c.id, s.term
            FROM skills s
            JOIN categories c ON c.name = s.category;
        DROP TABLE skills;
        ALTER TABLE skills_new RENAME TO skills;
    """)

    # Recreate llm_results with category_id
    conn.executescript("""
        CREATE TABLE llm_results_new (
            url_key     TEXT NOT NULL,
            url         TEXT NOT NULL,
            category_id INTEGER NOT NULL REFERENCES categories(id),
            skill       TEXT NOT NULL,
            is_matched  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (url_key, category_id, skill)
        );
        INSERT INTO llm_results_new(url_key, url, category_id, skill, is_matched)
            SELECT r.url_key, r.url, c.id, r.skill,
                   COALESCE(r.is_matched, 0)
            FROM llm_results r
            JOIN categories c ON c.name = r.category;
        DROP TABLE llm_results;
        ALTER TABLE llm_results_new RENAME TO llm_results;
    """)

    # Recreate skill_candidates with category_id + llm_category_id
    conn.executescript("""
        CREATE TABLE skill_candidates_new (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            term            TEXT NOT NULL,
            category_id     INTEGER NOT NULL REFERENCES categories(id),
            llm_category_id INTEGER NOT NULL REFERENCES categories(id),
            jobs_count      INTEGER DEFAULT 0,
            status          TEXT DEFAULT 'pending',
            added_date      TEXT NOT NULL,
            decided_date    TEXT,
            UNIQUE(term, category_id)
        );
        INSERT INTO skill_candidates_new(
            id, term, category_id, llm_category_id,
            jobs_count, status, added_date, decided_date
        )
            SELECT sc.id, sc.term, c.id, lc.id,
                   sc.jobs_count, sc.status, sc.added_date, sc.decided_date
            FROM skill_candidates sc
            JOIN categories c  ON c.name  = sc.category
            JOIN categories lc ON lc.name = sc.llm_category;
        DROP TABLE skill_candidates;
        ALTER TABLE skill_candidates_new RENAME TO skill_candidates;
    """)

    conn.commit()
    print("  [DB] Migrated to normalized category schema.")


def load_skills(conn: sqlite3.Connection) -> dict[str, list[tuple[str, str]]]:
    """Load skills catalog from DB including aliases.

    Returns {category: [(display_term, regex_pattern), ...]}
    - display_term: plain text name (e.g. 'c++')
    - regex_pattern: re.escape'd version for word-boundary matching (e.g. 'c\\+\\+')

    Aliases are included as extra patterns that map to the canonical display term.
    extract_skills() deduplicates by display so each canonical is counted once.
    """
    skills: dict[str, list[tuple[str, str]]] = {}

    for row in conn.execute("""
        SELECT c.name AS category, s.id, s.term
        FROM skills s
        JOIN categories c ON s.category_id = c.id
        ORDER BY c.name, s.id
    """):
        display = row["term"]
        pattern = re.escape(display)
        skills.setdefault(row["category"], []).append((display, pattern))

    # Aliases — extra regex patterns that resolve to the canonical display term
    for row in conn.execute("""
        SELECT c.name AS category, s.term AS canonical_term, a.canonical AS alias_plain
        FROM skill_aliases a
        JOIN skills s ON a.skill_id = s.id
        JOIN categories c ON s.category_id = c.id
    """):
        display = row["canonical_term"]
        pattern = re.escape(row["alias_plain"])
        skills.setdefault(row["category"], []).append((display, pattern))

    return skills
