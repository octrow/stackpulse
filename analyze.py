"""
Skills analysis on collected job JSON files.

Usage:
    python analyze.py                        # analyze today's file
    python analyze.py --file data/jobs_2026-04-01.json
    python analyze.py --all                  # merge all JSON files in data/
    python analyze.py --llm                  # also extract skills via LLM (free, via 9router)
    python analyze.py --all --llm
    python analyze.py --promote              # promote LLM-discovered candidates (≥2 jobs) into skills
    python analyze.py --promote 3            # same but threshold = 3 jobs
    python analyze.py --all --promote        # promote first, then analyze with enriched skills
    python analyze.py --candidates           # show pending skill candidates queue

DB: data/skills.db stores skills catalog + LLM results (auto-created on first run).
To add a term without touching code:
    sqlite3 data/skills.db "INSERT OR IGNORE INTO skills(category_id,term) SELECT id,'hetzner' FROM categories WHERE name='Cloud'"
To reject a candidate so it never gets promoted:
    sqlite3 data/skills.db "UPDATE skill_candidates SET status='rejected' WHERE term='<term>'"
To add an alias (e.g. German synonym):
    sqlite3 data/skills.db \\
      "INSERT INTO skill_aliases(skill_id,alias,canonical,lang,alias_type)
       SELECT id,'Deutsch','deutsch','de','translation' FROM skills WHERE term='german'"
"""

import argparse
import hashlib
import json
import re
import sqlite3
import time
from argparse import Namespace
from collections import Counter
from datetime import date
from pathlib import Path

import pandas as pd

from config import (
    OUTPUT_DIR,
    DB_FILENAME,
    NINEROUTER_BASE_URL,
    NINEROUTER_MODEL,
    NINEROUTER_FALLBACK_MODEL,
    LLM_RATE_LIMIT_MAX_WAIT_SECONDS,
    LLM_MAX_INPUT_CHARS,
    LLM_MAX_OUTPUT_TOKENS,
    RETRY_AFTER_BUFFER_SECONDS,
    LLM_CANDIDATE_THRESHOLD,
)

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

# ── Candidate promotion constants ─────────────────────────────────────────────

# Generic terms that should never enter the skills catalog regardless of frequency
SKIP_TERMS: set[str] = {
    "api",
    "testing",
    "automation",
    "debugging",
    "configuration",
    "scalability",
    "concurrency",
    "orchestration",
    "containerization",
    "profiling",
    "modular code",
    "testable code",
    "object-oriented programming",
    "async programming",
    "asynchronous programming",
    "error diagnosis",
    "database optimization",
    "memory optimization",
    "orm optimization",
    "restful apis",
    "rest apis",
    "ci/cd workflows",
    "ci/cd pipelines",
    "agentic systems",
    "autonomous ai agents",
}

# Maps LLM output category names → skills catalog categories (constant, not in DB)

# Initial alias seeds: (skill_term, alias_text, language, alias_type)
# Only seeded when skill_aliases table is empty.
# More aliases can be added via SQL (see module docstring).
ALIAS_SEED: list[tuple[str, str, str, str]] = [
    ("python", "python3", "en", "variant"),
    ("python", "python 3", "en", "variant"),
]

# ── DB safety constants ───────────────────────────────────────────────────────

_VALID_DB_TABLES: frozenset[str] = frozenset(
    {
        "categories",
        "skills",
        "llm_results",
        "skill_candidates",
        "skill_aliases",
    }
)

# ── Display formatting constants ──────────────────────────────────────────────

_REPORT_SKILL_WIDTH = 25
_REPORT_CATEGORY_WIDTH = 30
_REPORT_LOCATION_WIDTH = 35
_REPORT_TOP_SKILLS_COUNT = 30
_REPORT_TOP_CATEGORIES_COUNT = 8
_REPORT_TOP_LOCATIONS_COUNT = 15
_REPORT_TOP_SALARY_COUNT = 20
_REPORT_TOP_MISSING_SKILLS_COUNT = 50


# ── SQLite helpers ────────────────────────────────────────────────────────────


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


# ── Candidate pipeline ────────────────────────────────────────────────────────


def promote_llm_to_candidates(
    conn: sqlite3.Connection, threshold: int = LLM_CANDIDATE_THRESHOLD
) -> int:
    """Aggregate llm_results and queue new terms in skill_candidates.

    Only terms seen in >= threshold distinct jobs are added.
    Terms already in skills or in candidates (any status) are skipped.
    Existing candidates get their jobs_count updated if it has grown.

    Returns the number of newly added candidates.
    """
    existing_skill_terms: set[str] = {
        row["term"] for row in conn.execute("SELECT term FROM skills")
    }
    existing_alias_terms: set[str] = {
        row["canonical"] for row in conn.execute("SELECT canonical FROM skill_aliases")
    }
    known_terms = existing_skill_terms | existing_alias_terms

    # Existing candidates keyed by (term, category_id) → current jobs_count
    existing_candidates: dict[tuple[str, int], int] = {
        (row["term"], row["category_id"]): row["jobs_count"]
        for row in conn.execute(
            "SELECT term, category_id, jobs_count FROM skill_candidates"
        )
    }

    llm_rows = conn.execute(
        """
        SELECT r.skill, c.name AS category, r.category_id,
               COUNT(DISTINCT r.url_key) AS n
        FROM llm_results r
        JOIN categories c ON r.category_id = c.id
        WHERE r.is_matched = 0
        GROUP BY r.skill, r.category_id
        HAVING n >= ?
        ORDER BY n DESC
    """,
        (threshold,),
    ).fetchall()

    newly_added_count = updated_count = 0
    today = date.today().isoformat()

    for llm_row in llm_rows:
        skill: str = llm_row["skill"]
        category_id: int = llm_row["category_id"]
        jobs_count: int = llm_row["n"]

        normalized = normalize_term(skill)

        if normalized in SKIP_TERMS:
            continue

        if normalized in known_terms:
            continue

        candidate_key = (normalized, category_id)

        if candidate_key in existing_candidates:
            if jobs_count > existing_candidates[candidate_key]:
                conn.execute(
                    "UPDATE skill_candidates SET jobs_count=? WHERE term=? AND category_id=?",
                    (jobs_count, normalized, category_id),
                )
                updated_count += 1
        else:
            conn.execute(
                """INSERT OR IGNORE INTO skill_candidates
                   (term, category_id, llm_category_id, jobs_count, added_date)
                   VALUES (?,?,?,?,?)""",
                (normalized, category_id, category_id, jobs_count, today),
            )
            newly_added_count += 1
            existing_candidates[candidate_key] = jobs_count

    conn.commit()

    pending_count = conn.execute(
        "SELECT COUNT(*) FROM skill_candidates WHERE status='pending'"
    ).fetchone()[0]
    print(
        f"  [Candidates queue] {newly_added_count} new, {updated_count} updated "
        f"(threshold >=\u2009{threshold} jobs) \u2192 {pending_count} pending review rows"
    )
    return newly_added_count


def apply_candidates(conn: sqlite3.Connection, min_jobs: int = 2) -> int:
    """Promote pending candidates with jobs_count >= min_jobs into skills catalog.

    Returns the number of terms promoted.
    """
    candidate_rows = conn.execute(
        """SELECT sc.term, c.name AS category, sc.category_id, sc.jobs_count
           FROM skill_candidates sc
           JOIN categories c ON sc.category_id = c.id
           WHERE sc.status='pending' AND sc.jobs_count >= ?
           ORDER BY sc.jobs_count DESC""",
        (min_jobs,),
    ).fetchall()

    if not candidate_rows:
        print(f"  [Promote] No pending candidates with jobs_count >= {min_jobs}.")
        return 0

    print(
        f"\n  [Promote] Adding {len(candidate_rows)} terms to skills (threshold >=\u2009{min_jobs} jobs):"
    )
    print(f"  {'Term':<28} {'Category':<32} {'Jobs'}")
    print(f"  {'-' * 28} {'-' * 32} {'-' * 4}")
    today = date.today().isoformat()
    for row in candidate_rows:
        print(f"  {row['term']:<28} {row['category']:<32} {row['jobs_count']}")
        conn.execute(
            "INSERT OR IGNORE INTO skills(category_id, term) VALUES (?,?)",
            (row["category_id"], row["term"]),
        )
        conn.execute(
            """UPDATE skill_candidates
               SET status='approved', decided_date=?
               WHERE term=? AND category_id=?""",
            (today, row["term"], row["category_id"]),
        )

    conn.commit()
    return len(candidate_rows)


def print_candidates(conn: sqlite3.Connection) -> None:
    """Print the skill_candidates queue grouped by status."""
    rows = conn.execute(
        """SELECT sc.term, c.name AS category, lc.name AS llm_category,
                  sc.jobs_count, sc.status
           FROM skill_candidates sc
           JOIN categories c  ON sc.category_id     = c.id
           JOIN categories lc ON sc.llm_category_id = lc.id
           ORDER BY
               CASE sc.status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,
               sc.jobs_count DESC"""
    ).fetchall()

    if not rows:
        print("No skill candidates found. Run: py analyze.py --llm")
        return

    status_counts: Counter = Counter(row["status"] for row in rows)
    print(
        f"\nSkill candidates (all statuses): {len(rows)} total  "
        f"({status_counts.get('pending', 0)} pending, "
        f"{status_counts.get('approved', 0)} approved, "
        f"{status_counts.get('rejected', 0)} rejected)"
    )
    print(
        "  Note: this queue summary is separate from skills/alias coverage gaps shown in --llm output."
    )

    pending_rows = [row for row in rows if row["status"] == "pending"]
    if pending_rows:
        print(
            f"\n  {'Term':<25} {'Category':<{_REPORT_CATEGORY_WIDTH}} {'LLM Cat':<15} {'Jobs'}"
        )
        print(f"  {'-' * 25} {'-' * _REPORT_CATEGORY_WIDTH} {'-' * 15} {'-' * 4}")
        for row in pending_rows:
            print(
                f"  {row['term']:<25} {row['category']:<{_REPORT_CATEGORY_WIDTH}} "
                f"{row['llm_category']:<15} {row['jobs_count']}"
            )

    print("\nTo promote all pending (>= 2 jobs):   py analyze.py --promote")
    print(
        "To reject a term:  sqlite3 data/skills.db "
        "\"UPDATE skill_candidates SET status='rejected' WHERE term='<term>'\""
    )


# ── LLM extraction ────────────────────────────────────────────────────────────


def _build_llm_prompt(skills: dict[str, list[tuple[str, str]]]) -> str:
    """Build a skills-aware LLM prompt.

    Serializes existing skill terms grouped by category so the LLM can match
    against known terms and only flag genuinely new discoveries.
    """
    # Deduplicate display terms per category (aliases share the same display)
    lines = []
    categories_list = []
    for category, term_pairs in skills.items():
        unique_terms = sorted({display for display, _ in term_pairs})
        lines.append(f"  {category}: {', '.join(unique_terms)}")
        categories_list.append(category)

    skills_block = "\n".join(lines)
    categories_block = ", ".join(f'"{c}"' for c in categories_list)

    return f"""You are a skill extraction assistant. Below is a catalog of known technical skills grouped by category.

KNOWN SKILLS:
{skills_block}

TASK: Analyze the job description and:
1. List ALL known skill terms that are mentioned or clearly implied. Use the EXACT term from the catalog — do not paraphrase.
2. List any genuinely NEW technical skills/tools/protocols NOT in the catalog. For each, use the EXACT category name from this list — copy verbatim: {categories_block}.

Return ONLY JSON:
{{
  "matched": ["term1", "term2"],
  "new_terms": [{{"term": "newterm", "category": "Category Name"}}]
}}

Rules:
- "matched" must contain ONLY exact terms from the catalog above, lowercase
- "new_terms": only include specific, concrete technologies, tools, libraries, or protocols — NOT generic concepts like "debugging", "scalability", "containerization", "restful apis", "ci/cd pipelines", "async programming", "design patterns"
- Do NOT include soft skills or company names
- Return ONLY the JSON, no explanation

Job description:
"""


def _url_key(url: str) -> str:
    """Return an MD5 hex digest of the URL for use as a DB cache key."""
    return hashlib.md5(url.encode()).hexdigest()


def _llm_cache_get(
    conn: sqlite3.Connection, url_key: str
) -> dict[str, list[str]] | None:
    """Return cached LLM extraction results for url_key, or None if not cached.

    Returns {category: [skills]} with a special '_matched' key for is_matched=1 rows
    to maintain internal API compatibility with _build_comprehensive_by_category.
    """
    rows = conn.execute(
        """SELECT c.name AS category, r.skill, r.is_matched
           FROM llm_results r
           JOIN categories c ON r.category_id = c.id
           WHERE r.url_key = ?""",
        (url_key,),
    ).fetchall()
    if not rows:
        return None
    cached_skills: dict[str, list[str]] = {}
    for row in rows:
        if row["is_matched"]:
            cached_skills.setdefault("_matched", []).append(row["skill"])
        else:
            cached_skills.setdefault(row["category"], []).append(row["skill"])
    return cached_skills


def _llm_cache_set(
    conn: sqlite3.Connection,
    url: str,
    url_key: str,
    result: dict[str, list[str]],
) -> None:
    """Persist LLM extraction results to the DB cache.

    Entries under '_matched' key are stored with is_matched=1 and their actual
    skills-catalog category looked up from the skills table.
    """
    for category, skills in result.items():
        is_matched = 1 if category == "_matched" else 0
        for skill in skills:
            if is_matched:
                # Look up category_id from the skills table via join
                row = conn.execute(
                    """SELECT s.category_id FROM skills s WHERE s.term = ?""",
                    (skill.lower(),),
                ).fetchone()
                if row is None:
                    continue
                cat_id = row["category_id"]
            else:
                row = conn.execute(
                    "SELECT id FROM categories WHERE name = ?", (category,)
                ).fetchone()
                if row is None:
                    continue
                cat_id = row["id"]
            conn.execute(
                "INSERT OR IGNORE INTO llm_results"
                "(url_key, url, category_id, skill, is_matched) VALUES(?,?,?,?,?)",
                (url_key, url, cat_id, skill, is_matched),
            )
    conn.commit()


def _parse_retry_after(error_message: str) -> int | None:
    """Parse the suggested wait time (seconds) from a 429 error message.

    Handles both:
      "Please try again in 18m0.864s"  (groq TPD exhaustion)
      "reset after 1m 4s"              (per-minute window reset)
    Returns seconds rounded up plus RETRY_AFTER_BUFFER_SECONDS, or None if unparseable.
    """
    for pattern in (
        r"try again in (?:(\d+)m\s*)?(\d+(?:\.\d+)?)s",
        r"reset after (?:(\d+)m\s*)?(\d+(?:\.\d+)?)s",
    ):
        match = re.search(pattern, str(error_message))
        if match:
            minutes = int(match.group(1)) if match.group(1) else 0
            seconds = float(match.group(2))
            return int(minutes * 60 + seconds) + RETRY_AFTER_BUFFER_SECONDS
    return None


def _llm_call(client, model: str, prompt: str, text: str) -> dict:
    """Execute a single LLM extraction call and return parsed JSON.

    Raises on any error — callers handle retries and fallback.
    """
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt + text[:LLM_MAX_INPUT_CHARS]}],
        max_tokens=LLM_MAX_OUTPUT_TOKENS,
        temperature=0,
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def _call_llm_with_retry(
    client,
    model: str,
    prompt: str,
    text: str,
    url: str,
    attempt_label: str,
) -> dict | None:
    """Execute one LLM call with at most one retry on short 429 windows."""
    import json as _json

    from openai import APIConnectionError, APIError, RateLimitError

    try:
        return _llm_call(client, model, prompt, text)
    except RateLimitError as rate_limit_error:
        wait_seconds = _parse_retry_after(str(rate_limit_error))
        if wait_seconds is not None and wait_seconds <= LLM_RATE_LIMIT_MAX_WAIT_SECONDS:
            print(
                f"  [LLM] 429 on {attempt_label} — sleeping {wait_seconds}s then retrying …"
            )
            time.sleep(wait_seconds)
            try:
                return _llm_call(client, model, prompt, text)
            except RateLimitError as retry_error:
                print(f"  [LLM] 429 again after retry ({attempt_label}): {retry_error}")
                return None

        wait_display = f"{wait_seconds}s" if wait_seconds else "unknown"
        print(
            f"  [LLM] 429 on {attempt_label} — wait {wait_display} exceeds limit, "
            f"skipping primary"
        )
        return None
    except (APIError, APIConnectionError, _json.JSONDecodeError, ValueError) as error:
        print(f"  [LLM] Warning: extraction failed for {url[:60]}: {error}")
        return None


def _extract_skills_with_models(
    text: str,
    url: str,
    client,
    prompt: str,
) -> dict | None:
    """Run primary model, then optional fallback model on failure."""
    result = _call_llm_with_retry(
        client,
        NINEROUTER_MODEL,
        prompt,
        text,
        url,
        NINEROUTER_MODEL,
    )
    if result is not None:
        return result

    if not NINEROUTER_FALLBACK_MODEL:
        return None

    print(f"  [LLM] Trying fallback model: {NINEROUTER_FALLBACK_MODEL}")
    return _call_llm_with_retry(
        client,
        NINEROUTER_FALLBACK_MODEL,
        prompt,
        text,
        url,
        f"fallback:{NINEROUTER_FALLBACK_MODEL}",
    )


def _normalize_llm_result(
    raw_result: dict, skills: dict[str, list[tuple[str, str]]]
) -> dict[str, list[str]]:
    """Convert LLM output to internal cache format.

    LLM returns: {"matched": ["python", ...], "new_terms": [{"term": "x", "category": "Y"}, ...]}
    Internal: {"_matched": ["python", ...], "Category Name": ["new_term", ...]}

    The "_matched" key is an internal marker — _llm_cache_set translates it to
    is_matched=1 with the actual skills category when writing to DB.
    """
    if "matched" not in raw_result and "new_terms" not in raw_result:
        # Old-format LLM result — treat all as new discoveries
        return raw_result

    # Build set of valid skill display terms for validation
    valid_terms = {display for terms in skills.values() for display, _ in terms}

    normalized: dict[str, list[str]] = {}

    # Matched terms — validate against known skills
    matched = raw_result.get("matched", [])
    if matched:
        validated = [
            t for t in matched if isinstance(t, str) and t.lower() in valid_terms
        ]
        if validated:
            normalized["_matched"] = validated

    # New terms — store under their suggested category
    new_terms = raw_result.get("new_terms", [])
    for entry in new_terms:
        if not isinstance(entry, dict):
            continue
        term = entry.get("term", "")
        category = entry.get("category", "")
        if term and category:
            normalized.setdefault(category, []).append(term.lower())

    return normalized


def extract_skills_llm(
    text: str,
    url: str,
    conn: sqlite3.Connection,
    client,
    prompt: str = "",
    taxonomy: dict[str, list[tuple[str, str]]] | None = None,
) -> dict[str, list[str]]:
    """Call LLM via 9router to extract skills. Uses DB cache to avoid re-calls.

    On 429:
      - If the suggested wait is ≤ LLM_RATE_LIMIT_MAX_WAIT_SECONDS, sleeps and retries once.
      - If NINEROUTER_FALLBACK_MODEL is configured, tries that next.
      - Otherwise logs a warning and returns {}.
    """
    cache_key = _url_key(url)
    cached = _llm_cache_get(conn, cache_key)
    if cached is not None:
        return cached

    result = _extract_skills_with_models(text, url, client, prompt)
    if result is None:
        return {}

    # Normalize LLM output before caching
    if taxonomy is not None:
        result = _normalize_llm_result(result, taxonomy)

    _llm_cache_set(conn, url, cache_key, result)
    return result


# ── Regex-based taxonomy extraction ──────────────────────────────────────────


def extract_skills(
    text: str, taxonomy: dict[str, list[tuple[str, str]]]
) -> dict[str, list[str]]:
    """Match taxonomy terms and aliases against text.

    taxonomy format: {category: [(display_term, regex_pattern), ...]}
    Returns {category: [display_term, ...]} — aliases resolve to their canonical display term,
    duplicates within a category are suppressed.
    """
    text_lower = text.lower()
    matched_skills: dict[str, list[str]] = {}
    for category, term_pairs in taxonomy.items():
        deduplicated_displays: set[str] = set()
        category_hits: list[str] = []
        for display, pattern in term_pairs:
            if display not in deduplicated_displays and re.search(
                r"\b" + pattern + r"\b", text_lower
            ):
                deduplicated_displays.add(display)
                category_hits.append(display)
        if category_hits:
            matched_skills[category] = category_hits
    return matched_skills


# ── Data loading ──────────────────────────────────────────────────────────────


def load_jobs(paths: list[Path]) -> list[dict]:
    """Load and deduplicate jobs from one or more JSON files."""
    all_jobs = []
    for path in paths:
        with open(path, encoding="utf-8") as file_handle:
            all_jobs.extend(json.load(file_handle))
    seen_urls: set[str] = set()
    deduplicated_jobs = []
    for job in all_jobs:
        url = job.get("linkedin_url", "")
        if url not in seen_urls:
            seen_urls.add(url)
            deduplicated_jobs.append(job)
    return deduplicated_jobs


# ── Analysis ──────────────────────────────────────────────────────────────────


def _build_comprehensive_by_category(
    skills_found: dict[str, list[str]],
    llm_skills: dict[str, list[str]],
    skills_catalog: dict[str, list[tuple[str, str]]],
) -> dict[str, list[str]]:
    """Merge regex hits with LLM skills into unified per-category dict.

    Matched LLM terms (under '_matched' key) are routed to their catalog category
    via reverse lookup. New discoveries are stored under their LLM-suggested category.
    """
    merged: dict[str, list[str]] = {
        cat: list(hits) for cat, hits in skills_found.items()
    }

    if not llm_skills:
        return merged

    # Build reverse index: display_term -> category
    term_to_cat: dict[str, str] = {}
    for cat, term_pairs in skills_catalog.items():
        for display, _ in term_pairs:
            term_to_cat.setdefault(display, cat)

    for llm_cat, skills in llm_skills.items():
        for skill in skills:
            if llm_cat == "_matched":
                target_cat = term_to_cat.get(skill.lower())
                if target_cat is None:
                    continue
            else:
                target_cat = llm_cat
            existing = merged.setdefault(target_cat, [])
            if skill.lower() not in {s.lower() for s in existing}:
                existing.append(skill)

    return merged


def analyze(
    jobs: list[dict],
    taxonomy: dict[str, list[tuple[str, str]]],
    llm_client=None,
    conn: sqlite3.Connection | None = None,
) -> pd.DataFrame:
    """Build a DataFrame with per-job metadata and extracted skills."""
    # Build skills-aware prompt once for the entire run
    llm_prompt = _build_llm_prompt(taxonomy) if llm_client else ""

    job_rows = []
    for job in jobs:
        description = job.get("job_description") or ""
        title = job.get("job_title") or ""
        combined_text = f"{title} {description}"
        url = job.get("linkedin_url", "")

        skills_found = extract_skills(combined_text, taxonomy)

        llm_skills: dict[str, list[str]] = {}
        if llm_client and conn is not None:
            llm_skills = extract_skills_llm(
                combined_text,
                url,
                conn,
                llm_client,
                prompt=llm_prompt,
                taxonomy=taxonomy,
            )

        # Unified per-category dict (regex + LLM merged)
        skills_by_cat = _build_comprehensive_by_category(
            skills_found,
            llm_skills,
            taxonomy,
        )

        regex_skills_flat = [skill for hits in skills_found.values() for skill in hits]
        all_skills_comprehensive = [
            skill for hits in skills_by_cat.values() for skill in hits
        ]

        job_rows.append(
            {
                "job_title": job.get("job_title"),
                "company": job.get("company"),
                "location": job.get("location"),
                "search_location": job.get("search_location"),
                "posted_date": job.get("posted_date"),
                "salary_extracted": job.get("salary_extracted"),
                "linkedin_url": url,
                "scraped_date": job.get("scraped_date"),
                "applicant_count": job.get("applicant_count"),
                "skills_raw": skills_found,
                "skills_by_category": skills_by_cat,
                "all_skills_flat": regex_skills_flat,
                "all_skills_comprehensive": all_skills_comprehensive,
                "skills_llm": llm_skills,
                "has_description": bool(description.strip()),
            }
        )

    return pd.DataFrame(job_rows)


def _print_top_skills(df: pd.DataFrame) -> None:
    """Print top skill frequencies across all postings.

    Uses the comprehensive column (regex + LLM merged) when available.
    """
    col = (
        "all_skills_comprehensive"
        if "all_skills_comprehensive" in df.columns
        else "all_skills_flat"
    )
    all_skills: Counter = Counter()
    for skills_list in df[col]:
        all_skills.update(skills_list)

    has_llm = "skills_llm" in df.columns and df["skills_llm"].apply(bool).any()
    label = (
        "comprehensive (regex + LLM)"
        if col == "all_skills_comprehensive" and has_llm
        else "regex taxonomy"
    )
    print(f"\nTop {_REPORT_TOP_SKILLS_COUNT} skills [{label}] across all postings:")
    for skill, count in all_skills.most_common(_REPORT_TOP_SKILLS_COUNT):
        percentage = count / len(df) * 100
        percentage_bar = "█" * int(percentage / 2)
        print(
            f"  {skill:<{_REPORT_SKILL_WIDTH}} {count:>4} jobs  ({percentage:>5.1f}%)  {percentage_bar}"
        )


def _print_category_breakdown(
    df: pd.DataFrame,
    taxonomy: dict[str, list[tuple[str, str]]],
) -> None:
    """Print category-wise top terms (regex + LLM when available)."""
    col = "skills_by_category" if "skills_by_category" in df.columns else "skills_raw"
    skills_list: list[dict] = df[col].tolist()

    print("\nBy category:")
    # Collect all categories present across jobs (taxonomy order first, then extras)
    all_categories: list[str] = list(taxonomy.keys())
    extra = {cat for row in skills_list for cat in row if cat not in taxonomy}
    all_categories.extend(sorted(extra))

    for category in all_categories:
        category_counter: Counter = Counter()
        for skills_row in skills_list:
            if category in skills_row:
                category_counter.update(skills_row[category])
        if not category_counter:
            continue

        total_jobs = len(df)
        top_terms = ", ".join(
            f"{term}({count}, {count / total_jobs * 100:.0f}%)"
            for term, count in category_counter.most_common(
                _REPORT_TOP_CATEGORIES_COUNT
            )
        )
        print(f"  {category:<{_REPORT_CATEGORY_WIDTH}} {top_terms}")


def _print_top_locations(df: pd.DataFrame) -> None:
    """Print the most frequent locations in scraped results."""
    print("\nTop locations in results:")
    for location, count in (
        df["location"].value_counts().head(_REPORT_TOP_LOCATIONS_COUNT).items()
    ):
        print(f"  {location:<{_REPORT_LOCATION_WIDTH}} {count}")


def _print_salary_hints(df: pd.DataFrame) -> None:
    """Print postings where a salary hint was extracted."""
    salary_rows = df[df["salary_extracted"].notna()]
    print(f"\nSalary hints found in {len(salary_rows)}/{len(df)} postings:")
    for _, row in salary_rows.head(_REPORT_TOP_SALARY_COUNT).iterrows():
        label = row["job_title"] or row.get("company") or "N/A"
        location = row.get("search_location") or row.get("location") or ""
        loc_hint = f" [{location}]" if location else ""
        print(f"  {label:<40}{loc_hint:<25} {row['salary_extracted']}")


def _known_skill_terms(skills: dict[str, list[tuple[str, str]]]) -> set[str]:
    """Return normalized skill terms (including aliases) for membership checks."""
    return {display for terms in skills.values() for display, _ in terms}


def _count_missing_skill_terms(
    skills_llm_list: list[dict],
    known_terms: set[str],
) -> Counter:
    """Count LLM terms absent from current skills/alias coverage.

    Only non-matched entries (new discoveries) are considered missing.
    """
    missing: Counter = Counter()
    for llm_skills in skills_llm_list:
        if not llm_skills:
            continue
        for cat, skills in llm_skills.items():
            if cat == "_matched":
                continue
            for skill in skills:
                normalized = normalize_term(skill)
                if normalized not in known_terms:
                    missing[normalized] += 1
    return missing


def _build_actionable_missing_terms(
    skills_missing: Counter,
    existing_candidate_terms: set[str],
    threshold: int,
) -> Counter:
    """Filter uncovered terms to queue-actionable terms."""
    skip_normalized = {normalize_term(term) for term in SKIP_TERMS}
    return Counter(
        {
            term: count
            for term, count in skills_missing.items()
            if count >= threshold
            and term not in skip_normalized
            and term not in existing_candidate_terms
        }
    )


def _print_missing_skill_terms(
    skills_missing: Counter,
    actionable_missing: Counter,
    threshold: int,
) -> None:
    """Print uncovered LLM terms and queue-actionable subset."""
    if not skills_missing:
        return

    print(
        f"\nLLM terms not covered by current skills/aliases "
        f"({len(skills_missing)} terms):"
    )
    print(
        f"  Actionable for queue (threshold >= {threshold}, not SKIP_TERMS, not already queued): "
        f"{len(actionable_missing)} terms"
    )

    for skill, count in skills_missing.most_common(_REPORT_TOP_MISSING_SKILLS_COUNT):
        print(f"  {skill:<{_REPORT_SKILL_WIDTH}} {count} jobs")

    if actionable_missing:
        print("\nTop actionable uncovered terms:")
        for skill, count in actionable_missing.most_common(
            _REPORT_TOP_MISSING_SKILLS_COUNT
        ):
            print(f"  {skill:<{_REPORT_SKILL_WIDTH}} {count} jobs")

    print("  Note: raw uncovered count is broader than pending queue by design.")


def _print_llm_section(
    df: pd.DataFrame,
    skills_catalog: dict[str, list[tuple[str, str]]],
    existing_candidate_terms: set[str],
    candidate_threshold: int,
) -> None:
    """Print skills coverage gaps discovered by LLM extraction."""
    if "skills_llm" not in df.columns or not df["skills_llm"].apply(bool).any():
        return

    print(f"\n{'=' * 60}")
    print("SKILLS COVERAGE GAPS (LLM-discovered)")
    print(f"{'=' * 60}")

    skills_llm_list: list[dict] = df["skills_llm"].tolist()

    known_terms = _known_skill_terms(skills_catalog)
    skills_missing = _count_missing_skill_terms(
        skills_llm_list,
        known_terms,
    )
    actionable_missing = _build_actionable_missing_terms(
        skills_missing,
        existing_candidate_terms,
        candidate_threshold,
    )
    _print_missing_skill_terms(
        skills_missing,
        actionable_missing,
        candidate_threshold,
    )


def _print_quality_summary(df: pd.DataFrame) -> None:
    """Print extraction quality counters."""
    total = len(df)
    no_desc = (~df["has_description"]).sum() if "has_description" in df.columns else 0
    no_skills = (df["all_skills_flat"].apply(len) == 0).sum()
    print(f"\nExtraction quality ({total} jobs):")
    print(f"  Empty description  : {no_desc:>4} ({no_desc / total * 100:.1f}%)")
    print(f"  Zero skills found  : {no_skills:>4} ({no_skills / total * 100:.1f}%)")


def _print_skills_by_location(df: pd.DataFrame) -> None:
    """Print top 3 skills per unique search_location."""
    if "search_location" not in df.columns:
        return
    locations = df["search_location"].dropna().unique()
    if len(locations) <= 1:
        return
    col = (
        "all_skills_comprehensive"
        if "all_skills_comprehensive" in df.columns
        else "all_skills_flat"
    )
    print("\nTop skills by search location:")
    for loc in sorted(locations):
        subset = df[df["search_location"] == loc]
        counter: Counter = Counter()
        for skills_list in subset[col]:
            counter.update(skills_list)
        top = ", ".join(f"{s}({c})" for s, c in counter.most_common(3))
        print(f"  {str(loc):<30} {top}")


def print_report(
    df: pd.DataFrame,
    taxonomy: dict[str, list[tuple[str, str]]],
    existing_candidate_canonicals: set[str],
    candidate_threshold: int,
) -> None:
    """Print a human-readable frequency analysis to stdout."""
    print(f"\n{'=' * 60}")
    print(f"SKILLS ANALYSIS — {len(df)} unique job postings")
    print(f"{'=' * 60}")

    _print_quality_summary(df)
    _print_top_skills(df)
    _print_category_breakdown(df, taxonomy)
    _print_top_locations(df)
    _print_skills_by_location(df)
    _print_salary_hints(df)
    _print_llm_section(df, taxonomy, existing_candidate_canonicals, candidate_threshold)


def save_excel(
    df: pd.DataFrame,
    output_path: Path,
    taxonomy: dict[str, list[tuple[str, str]]],
) -> None:
    """Export the analysis DataFrame to Excel with one column per skill category."""
    internal_columns = [
        "skills_raw",
        "skills_by_category",
        "all_skills_flat",
        "all_skills_comprehensive",
        "skills_llm",
        "has_description",
    ]
    export_df = df.drop(columns=[col for col in internal_columns if col in df.columns])

    source_col = (
        "skills_by_category" if "skills_by_category" in df.columns else "skills_raw"
    )
    # Collect all categories across jobs (taxonomy order first, then extras from LLM)
    all_categories: list[str] = list(taxonomy.keys())
    if source_col == "skills_by_category":
        extra = {cat for row in df[source_col] for cat in row if cat not in taxonomy}
        all_categories.extend(sorted(extra))

    for category in all_categories:
        export_df[category] = df[source_col].apply(
            lambda raw, cat=category: ", ".join(raw.get(cat, []))
        )

    export_df.to_excel(output_path, index=False)
    print(f"\nExcel saved → {output_path.resolve()}")


# ── Entry point helpers ───────────────────────────────────────────────────────


def resolve_input_paths(args: Namespace, data_dir: Path) -> list[Path] | None:
    """Determine which JSON file(s) to analyze based on CLI arguments.

    Returns a list of Paths, or None if no files are found and execution should stop.
    """
    if args.file:
        return [Path(args.file)]

    if args.all:
        paths = sorted(data_dir.glob("jobs_*.json"))
        if not paths:
            print("No job files found in data/. Run scrape.py first.")
            return None
        return paths

    # Default: today's file, or the latest available
    today_file = data_dir / f"jobs_{date.today().isoformat()}.json"
    if today_file.exists():
        return [today_file]

    all_files = sorted(data_dir.glob("jobs_*.json"))
    if not all_files:
        print("No job files found. Run scrape.py first.")
        return None

    latest_file = all_files[-1]
    print(f"Today's file not found, using latest: {latest_file}")
    return [latest_file]


def build_llm_client(base_url: str, model: str):
    """Initialise and return an OpenAI-compatible client for 9router, or None on failure."""
    try:
        from openai import OpenAI

        client = OpenAI(base_url=base_url, api_key="local")
        print(f"LLM extraction enabled → {model} via 9router")
        return client
    except ImportError:
        print("openai package not installed. Run: pip install openai")
        return None


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    """Parse CLI arguments and run the requested analysis / promotion workflow."""
    parser = argparse.ArgumentParser(description="Analyze scraped LinkedIn jobs")
    parser.add_argument("--file", type=str, help="Specific JSON file to analyze")
    parser.add_argument(
        "--all", action="store_true", help="Merge all JSON files in data/"
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Also extract skills via LLM (free, uses 9router at localhost:20128)",
    )
    parser.add_argument(
        "--promote",
        nargs="?",
        const=2,
        type=int,
        default=None,
        metavar="N",
        help="Promote pending LLM candidates with jobs_count >= N (default 2) into skills",
    )
    parser.add_argument(
        "--candidates",
        action="store_true",
        help="Show pending skill candidates queue (no analysis)",
    )
    args = parser.parse_args()

    data_dir = Path(OUTPUT_DIR)
    conn = open_db(data_dir)
    try:
        init_db(conn)

        # Standalone: inspect candidates queue only
        if args.candidates:
            print_candidates(conn)
            return

        # promote-only mode: no job files needed
        promote_only = args.promote is not None and not args.file and not args.all
        if promote_only:
            apply_candidates(conn, max(args.promote, 1))
            return

        # Resolve which job file(s) to load
        paths = resolve_input_paths(args, data_dir)
        if paths is None:
            # No files found; still run promote if requested
            if args.promote is not None:
                apply_candidates(conn, max(args.promote, 1))
            return

        # Promote before loading skills so analysis uses the enriched term set
        if args.promote is not None:
            apply_candidates(conn, max(args.promote, 1))

        skills = load_skills(conn)
        term_count = sum(len(terms) for terms in skills.values())
        print(
            f"Skills loaded: {term_count} terms (+ aliases) across {len(skills)} categories"
        )

        print(f"Loading from: {[str(p) for p in paths]}")
        jobs = load_jobs(paths)
        print(f"Loaded {len(jobs)} unique jobs.")

        if not jobs:
            return

        llm_client = None
        if args.llm:
            llm_client = build_llm_client(NINEROUTER_BASE_URL, NINEROUTER_MODEL)

        df = analyze(jobs, skills, llm_client=llm_client, conn=conn)

        # After LLM extraction, auto-queue newly discovered terms as candidates
        if args.llm and llm_client:
            promote_llm_to_candidates(conn, threshold=LLM_CANDIDATE_THRESHOLD)

        existing_candidate_terms = {
            row["term"] for row in conn.execute("SELECT term FROM skill_candidates")
        }
    finally:
        conn.close()

    print_report(
        df,
        skills,
        existing_candidate_terms,
        candidate_threshold=LLM_CANDIDATE_THRESHOLD,
    )

    output_stem = paths[0].stem if len(paths) == 1 else "jobs_all"
    save_excel(df, data_dir / f"{output_stem}_analysis.xlsx", skills)


if __name__ == "__main__":
    main()
