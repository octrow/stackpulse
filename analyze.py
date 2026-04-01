"""
Skills analysis on collected job JSON files.

Usage:
    python analyze.py                        # analyze today's file
    python analyze.py --file data/jobs_2026-04-01.json
    python analyze.py --all                  # merge all JSON files in data/
    python analyze.py --llm                  # also extract skills via LLM (free, via 9router)
    python analyze.py --all --llm
    python analyze.py --promote              # promote LLM-discovered candidates (≥2 jobs) into taxonomy
    python analyze.py --promote 3            # same but threshold = 3 jobs
    python analyze.py --all --promote        # promote first, then analyze with enriched taxonomy
    python analyze.py --candidates           # show pending taxonomy candidates queue

DB: data/skills.db stores taxonomy + LLM results (auto-created on first run).
To add a term to the taxonomy without touching code:
    sqlite3 data/skills.db "INSERT OR IGNORE INTO taxonomy(category,term) VALUES('Cloud','hetzner')"
To reject a candidate so it never gets promoted:
    sqlite3 data/skills.db "UPDATE taxonomy_candidates SET status='rejected' WHERE canonical='<term>'"
To add an alias (e.g. German synonym):
    sqlite3 data/skills.db \\
      "INSERT INTO term_aliases(taxonomy_id,alias,canonical,lang,alias_type)
       SELECT id,'Deutsch','deutsch','de','translation' FROM taxonomy WHERE term='german'"
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
)

# ── Seed taxonomy (used only to initialise the DB on first run) ───────────────

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
        "c\\+\\+",
        "c#",
        "ruby",
        "php",
        "elixir",
        "bash",
        "sql",
        "c",
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

# Generic terms that should never enter the taxonomy regardless of frequency
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

# Maps LLM output category names → taxonomy categories (seeded into DB once)
LLM_CAT_SEED: list[tuple[str, str]] = [
    ("languages", "Languages"),
    ("frameworks", "Python Frameworks"),
    ("libraries", "Python Libraries"),
    ("databases", "Databases — Relational"),  # refined per-term by _DB_HINTS
    ("cloud_services", "Cloud"),
    ("devops", "IaC & CI/CD"),
    ("tools", "Containers & Orchestration"),
    ("concepts", "API & Architecture"),
]

# Keyword hints to pick the right DB sub-category when LLM says "databases"
_DB_HINTS: list[tuple[set[str], str]] = [
    (
        {
            "postgresql",
            "postgres",
            "mysql",
            "mariadb",
            "aurora",
            "sqlite",
            "cockroach",
            "tidb",
            "rds",
        },
        "Databases — Relational",
    ),
    (
        {
            "redis",
            "mongo",
            "elastic",
            "opensearch",
            "dynamo",
            "cassandra",
            "neo4j",
            "firestore",
            "couchdb",
            "nosql",
        },
        "Databases — NoSQL / Search",
    ),
    (
        {
            "bigquery",
            "snowflake",
            "redshift",
            "databricks",
            "clickhouse",
            "dbt",
            "iceberg",
            "duckdb",
            "data lake",
        },
        "Databases — Analytical",
    ),
]

# Initial alias seeds: (taxonomy_term, alias_text, language, alias_type)
# Only seeded when term_aliases table is empty.
# More aliases can be added via SQL (see module docstring).
ALIAS_SEED: list[tuple[str, str, str, str]] = [
    ("python", "python3", "en", "variant"),
    ("python", "python 3", "en", "variant"),
]

# ── Display formatting constants ──────────────────────────────────────────────

_REPORT_SKILL_WIDTH = 25
_REPORT_CATEGORY_WIDTH = 30
_REPORT_LOCATION_WIDTH = 35
_REPORT_LLM_CATEGORY_WIDTH = 20
_REPORT_TOP_SKILLS_COUNT = 30
_REPORT_TOP_CATEGORIES_COUNT = 8
_REPORT_TOP_LOCATIONS_COUNT = 15
_REPORT_TOP_SALARY_COUNT = 20
_REPORT_TOP_LLM_SKILLS_COUNT = 15
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
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] == 0


def normalize_term(term: str) -> str:
    """Lowercase, collapse whitespace, and regex-escape a term for taxonomy storage.

    The returned string is safe to embed in r'\\b' + term + r'\\b' patterns
    and can be stored directly in taxonomy.term or taxonomy_candidates.canonical.
    """
    normalised = term.strip().lower()
    normalised = re.sub(r"\s+", " ", normalised)
    return re.escape(normalised)


def resolve_category(llm_category: str, term: str, category_map: dict[str, str]) -> str:
    """Map an LLM category + term to the best-matching taxonomy category."""
    if llm_category == "databases":
        term_lower = term.lower()
        for keywords, taxonomy_category in _DB_HINTS:
            if any(keyword in term_lower for keyword in keywords):
                return taxonomy_category
        return "Databases — Relational"  # fallback for unknown DB terms
    return category_map.get(llm_category, "API & Architecture")


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables; seed taxonomy, llm_category_map, and term_aliases on first run."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS taxonomy (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            term     TEXT NOT NULL,
            UNIQUE(category, term)
        );

        CREATE TABLE IF NOT EXISTS llm_results (
            url_key  TEXT NOT NULL,
            url      TEXT NOT NULL,
            category TEXT NOT NULL,
            skill    TEXT NOT NULL,
            PRIMARY KEY (url_key, category, skill)
        );

        -- Maps LLM output category names → taxonomy categories
        CREATE TABLE IF NOT EXISTS llm_category_map (
            llm_category      TEXT PRIMARY KEY,
            taxonomy_category TEXT NOT NULL
        );

        -- Review queue: LLM-discovered terms pending taxonomy inclusion
        CREATE TABLE IF NOT EXISTS taxonomy_candidates (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            term              TEXT NOT NULL,
            canonical         TEXT NOT NULL,
            taxonomy_category TEXT NOT NULL,
            llm_category      TEXT NOT NULL,
            jobs_count        INTEGER DEFAULT 0,
            status            TEXT DEFAULT 'pending',
            added_date        TEXT NOT NULL,
            UNIQUE(canonical, taxonomy_category)
        );

        -- Aliases and multilingual synonyms for existing taxonomy terms
        CREATE TABLE IF NOT EXISTS term_aliases (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            taxonomy_id  INTEGER NOT NULL REFERENCES taxonomy(id) ON DELETE CASCADE,
            alias        TEXT NOT NULL,
            canonical    TEXT NOT NULL,
            lang         TEXT DEFAULT 'en',
            alias_type   TEXT DEFAULT 'variant',
            UNIQUE(canonical, taxonomy_id)
        );

        CREATE INDEX IF NOT EXISTS idx_aliases_canonical ON term_aliases(canonical);
    """)
    conn.commit()

    if _table_is_empty(conn, "taxonomy"):
        seed_rows = [
            (category, term)
            for category, terms in SKILLS_SEED.items()
            for term in terms
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO taxonomy(category, term) VALUES(?, ?)", seed_rows
        )
        conn.commit()
        print(f"  [DB] Seeded taxonomy with {len(seed_rows)} terms.")

    if _table_is_empty(conn, "llm_category_map"):
        conn.executemany(
            "INSERT OR IGNORE INTO llm_category_map VALUES (?,?)", LLM_CAT_SEED
        )
        conn.commit()

    if _table_is_empty(conn, "term_aliases"):
        for tax_term, alias_text, lang, alias_type in ALIAS_SEED:
            row = conn.execute(
                "SELECT id FROM taxonomy WHERE term=?", (tax_term,)
            ).fetchone()
            if row:
                canonical = normalize_term(alias_text)
                conn.execute(
                    """INSERT OR IGNORE INTO term_aliases
                       (taxonomy_id, alias, canonical, lang, alias_type)
                       VALUES (?,?,?,?,?)""",
                    (row["id"], alias_text, canonical, lang, alias_type),
                )
        conn.commit()


def load_taxonomy(conn: sqlite3.Connection) -> dict[str, list[tuple[str, str]]]:
    """Load taxonomy from DB including aliases.

    Returns {category: [(display_term, regex_pattern), ...]}
    - display_term: human-readable name reported in output (e.g. 'c++')
    - regex_pattern: string embedded in r'\\b' + pattern + r'\\b' (e.g. 'c\\+\\+')

    Aliases are included as extra patterns that map to the canonical display term.
    extract_skills() deduplicates by display so each canonical is counted once.
    """
    taxonomy: dict[str, list[tuple[str, str]]] = {}

    def _unescape(term: str) -> str:
        """Convert a regex-escaped term back to its human-readable display form."""
        return re.sub(r"\\(.)", r"\1", term)

    for row in conn.execute(
        "SELECT id, category, term FROM taxonomy ORDER BY category, id"
    ):
        display = _unescape(row["term"])
        taxonomy.setdefault(row["category"], []).append((display, row["term"]))

    # Aliases — extra regex patterns that resolve to the canonical display term
    for row in conn.execute("""
        SELECT t.category, t.term AS canonical_term, a.canonical AS alias_pattern
        FROM term_aliases a
        JOIN taxonomy t ON a.taxonomy_id = t.id
    """):
        display = _unescape(row["canonical_term"])
        taxonomy.setdefault(row["category"], []).append((display, row["alias_pattern"]))

    return taxonomy


# ── Candidate pipeline ────────────────────────────────────────────────────────


def _taxonomy_plain_key(term: str) -> str:
    """Plain-text key for taxonomy existence checks.

    Undoes regex escaping (e.g. c\\+\\+ → c++) and lowercases, so that
    LLM-returned plain strings can be compared against stored regex patterns.
    """
    return re.sub(r"\\(.)", r"\1", term).lower().strip()


def promote_llm_to_candidates(conn: sqlite3.Connection, threshold: int = 2) -> int:
    """Aggregate llm_results and queue new terms in taxonomy_candidates.

    Only terms seen in >= threshold distinct jobs are added.
    Terms already in taxonomy or in candidates (any status) are skipped.
    Existing candidates get their jobs_count updated if it has grown.

    Returns the number of newly added candidates.
    """
    category_map = dict(
        conn.execute(
            "SELECT llm_category, taxonomy_category FROM llm_category_map"
        ).fetchall()
    )

    # Taxonomy existence check uses plain-text keys (re.escape undone) so that
    # multi-word terms like "github actions" and C-style terms like c\+\+ are
    # correctly matched against plain LLM output.
    existing_taxonomy_keys: set[str] = {
        _taxonomy_plain_key(row["term"])
        for row in conn.execute("SELECT term FROM taxonomy")
    }
    # Aliases are already stored as normalize_term() output — compare by canonical
    existing_alias_canonicals: set[str] = {
        row["canonical"] for row in conn.execute("SELECT canonical FROM term_aliases")
    }

    known_taxonomy_terms = existing_taxonomy_keys | existing_alias_canonicals

    # Existing candidates keyed by (canonical, taxonomy_category) → current jobs_count
    existing_candidates: dict[tuple[str, str], int] = {
        (row["canonical"], row["taxonomy_category"]): row["jobs_count"]
        for row in conn.execute(
            "SELECT canonical, taxonomy_category, jobs_count FROM taxonomy_candidates"
        )
    }

    llm_rows = conn.execute(
        """
        SELECT skill, category, COUNT(DISTINCT url_key) AS n
        FROM llm_results
        GROUP BY skill, category
        HAVING n >= ?
        ORDER BY n DESC
    """,
        (threshold,),
    ).fetchall()

    newly_added_count = updated_count = 0
    today = date.today().isoformat()

    for llm_row in llm_rows:
        skill: str = llm_row["skill"]
        llm_category: str = llm_row["category"]
        jobs_count: int = llm_row["n"]

        if skill.lower() in SKIP_TERMS:
            continue

        canonical = normalize_term(skill)
        taxonomy_key = _taxonomy_plain_key(skill)

        if canonical in known_taxonomy_terms or taxonomy_key in known_taxonomy_terms:
            continue

        taxonomy_category = resolve_category(llm_category, skill, category_map)
        candidate_key = (canonical, taxonomy_category)

        if candidate_key in existing_candidates:
            if jobs_count > existing_candidates[candidate_key]:
                conn.execute(
                    "UPDATE taxonomy_candidates SET jobs_count=? WHERE canonical=? AND taxonomy_category=?",
                    (jobs_count, canonical, taxonomy_category),
                )
                updated_count += 1
        else:
            conn.execute(
                """INSERT OR IGNORE INTO taxonomy_candidates
                   (term, canonical, taxonomy_category, llm_category, jobs_count, added_date)
                   VALUES (?,?,?,?,?,?)""",
                (skill, canonical, taxonomy_category, llm_category, jobs_count, today),
            )
            newly_added_count += 1
            existing_candidates[candidate_key] = jobs_count

    conn.commit()

    pending_count = conn.execute(
        "SELECT COUNT(*) FROM taxonomy_candidates WHERE status='pending'"
    ).fetchone()[0]
    print(
        f"  [Candidates queue] {newly_added_count} new, {updated_count} updated "
        f"(threshold >=\u2009{threshold} jobs) \u2192 {pending_count} pending review rows"
    )
    return newly_added_count


def apply_candidates(conn: sqlite3.Connection, min_jobs: int = 2) -> int:
    """Promote pending candidates with jobs_count >= min_jobs into taxonomy.

    Returns the number of terms promoted.
    """
    candidate_rows = conn.execute(
        """SELECT term, canonical, taxonomy_category, jobs_count
           FROM taxonomy_candidates
           WHERE status='pending' AND jobs_count >= ?
           ORDER BY jobs_count DESC""",
        (min_jobs,),
    ).fetchall()

    if not candidate_rows:
        print(f"  [Promote] No pending candidates with jobs_count >= {min_jobs}.")
        return 0

    print(
        f"\n  [Promote] Adding {len(candidate_rows)} terms to taxonomy (threshold >=\u2009{min_jobs} jobs):"
    )
    print(f"  {'Term':<28} {'Category':<32} {'Jobs'}")
    print(f"  {'-' * 28} {'-' * 32} {'-' * 4}")
    for row in candidate_rows:
        print(f"  {row['term']:<28} {row['taxonomy_category']:<32} {row['jobs_count']}")
        conn.execute(
            "INSERT OR IGNORE INTO taxonomy(category, term) VALUES (?,?)",
            (row["taxonomy_category"], row["canonical"]),
        )
        conn.execute(
            """UPDATE taxonomy_candidates
               SET status='approved'
               WHERE canonical=? AND taxonomy_category=?""",
            (row["canonical"], row["taxonomy_category"]),
        )

    conn.commit()
    return len(candidate_rows)


def print_candidates(conn: sqlite3.Connection) -> None:
    """Print the taxonomy_candidates queue grouped by status."""
    rows = conn.execute(
        """SELECT term, canonical, taxonomy_category, llm_category, jobs_count, status
           FROM taxonomy_candidates
           ORDER BY
               CASE status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,
               jobs_count DESC"""
    ).fetchall()

    if not rows:
        print("No taxonomy candidates found. Run: py analyze.py --llm")
        return

    status_counts: Counter = Counter(row["status"] for row in rows)
    print(
        f"\nTaxonomy candidates (all statuses): {len(rows)} total  "
        f"({status_counts.get('pending', 0)} pending, "
        f"{status_counts.get('approved', 0)} approved, "
        f"{status_counts.get('rejected', 0)} rejected)"
    )
    print(
        "  Note: this queue summary is separate from taxonomy/alias coverage gaps shown in --llm output."
    )

    pending_rows = [row for row in rows if row["status"] == "pending"]
    if pending_rows:
        print(
            f"\n  {'Term':<25} {'Category':<{_REPORT_CATEGORY_WIDTH}} {'LLM Cat':<15} {'Jobs'}"
        )
        print(f"  {'-' * 25} {'-' * _REPORT_CATEGORY_WIDTH} {'-' * 15} {'-' * 4}")
        for row in pending_rows:
            print(
                f"  {row['term']:<25} {row['taxonomy_category']:<{_REPORT_CATEGORY_WIDTH}} "
                f"{row['llm_category']:<15} {row['jobs_count']}"
            )

    print("\nTo promote all pending (>= 2 jobs):   py analyze.py --promote")
    print(
        "To reject a term:  sqlite3 data/skills.db "
        "\"UPDATE taxonomy_candidates SET status='rejected' WHERE canonical='<term>'\""
    )


# ── LLM extraction ────────────────────────────────────────────────────────────

LLM_PROMPT = """Extract ALL hard technical skills from this job description.
Return ONLY a JSON object with these keys (use empty lists if none found):
{
  "languages": [],
  "frameworks": [],
  "libraries": [],
  "databases": [],
  "cloud_services": [],
  "tools": [],
  "concepts": [],
  "devops": []
}

Rules:
- Use lowercase, canonical names (e.g. "postgresql" not "Postgres", "fastapi" not "FastAPI")
- Be exhaustive — include everything technical mentioned
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
    """Return cached LLM extraction results for url_key, or None if not cached."""
    rows = conn.execute(
        "SELECT category, skill FROM llm_results WHERE url_key = ?", (url_key,)
    ).fetchall()
    if not rows:
        return None
    cached_skills: dict[str, list[str]] = {}
    for row in rows:
        cached_skills.setdefault(row["category"], []).append(row["skill"])
    return cached_skills


def _llm_cache_set(
    conn: sqlite3.Connection, url: str, url_key: str, result: dict[str, list[str]]
) -> None:
    """Persist LLM extraction results to the DB cache."""
    cache_rows = [
        (url_key, url, category, skill)
        for category, skills in result.items()
        for skill in skills
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO llm_results(url_key, url, category, skill) VALUES(?,?,?,?)",
        cache_rows,
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


def _llm_call(client, model: str, text: str) -> dict[str, list[str]]:
    """Execute a single LLM extraction call and return parsed JSON.

    Raises on any error — callers handle retries and fallback.
    """
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": LLM_PROMPT + text[:LLM_MAX_INPUT_CHARS]}],
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
    text: str,
    url: str,
    attempt_label: str,
) -> dict[str, list[str]] | None:
    """Execute one LLM call with at most one retry on short 429 windows."""
    from openai import RateLimitError

    try:
        return _llm_call(client, model, text)
    except RateLimitError as rate_limit_error:
        wait_seconds = _parse_retry_after(str(rate_limit_error))
        if wait_seconds is not None and wait_seconds <= LLM_RATE_LIMIT_MAX_WAIT_SECONDS:
            print(
                f"  [LLM] 429 on {attempt_label} — sleeping {wait_seconds}s then retrying …"
            )
            try:
                time.sleep(wait_seconds)
            except KeyboardInterrupt:
                raise
            try:
                return _llm_call(client, model, text)
            except RateLimitError as retry_error:
                print(f"  [LLM] 429 again after retry ({attempt_label}): {retry_error}")
                return None

        wait_display = f"{wait_seconds}s" if wait_seconds else "unknown"
        print(
            f"  [LLM] 429 on {attempt_label} — wait {wait_display} exceeds limit, "
            f"skipping primary"
        )
        return None
    except Exception as error:
        print(f"  [LLM] Warning: extraction failed for {url[:60]}: {error}")
        return None


def _extract_skills_with_models(
    text: str,
    url: str,
    client,
) -> dict[str, list[str]] | None:
    """Run primary model, then optional fallback model on failure."""
    result = _call_llm_with_retry(client, NINEROUTER_MODEL, text, url, NINEROUTER_MODEL)
    if result is not None:
        return result

    if not NINEROUTER_FALLBACK_MODEL:
        return None

    print(f"  [LLM] Trying fallback model: {NINEROUTER_FALLBACK_MODEL}")
    return _call_llm_with_retry(
        client,
        NINEROUTER_FALLBACK_MODEL,
        text,
        url,
        f"fallback:{NINEROUTER_FALLBACK_MODEL}",
    )


def extract_skills_llm(
    text: str, url: str, conn: sqlite3.Connection, client
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

    result = _extract_skills_with_models(text, url, client)
    if result is None:
        return {}

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


def analyze(
    jobs: list[dict],
    taxonomy: dict[str, list[tuple[str, str]]],
    llm_client=None,
    conn: sqlite3.Connection | None = None,
) -> pd.DataFrame:
    """Build a DataFrame with per-job metadata and extracted skills."""
    job_rows = []
    for job in jobs:
        description = job.get("job_description") or ""
        title = job.get("job_title") or ""
        combined_text = f"{title} {description}"
        url = job.get("linkedin_url", "")

        skills_found = extract_skills(combined_text, taxonomy)

        llm_skills: dict[str, list[str]] = {}
        if llm_client and conn is not None:
            llm_skills = extract_skills_llm(combined_text, url, conn, llm_client)

        regex_skills_flat = [skill for hits in skills_found.values() for skill in hits]
        llm_skills_flat = [skill for skills in llm_skills.values() for skill in skills]
        seen_lower: set[str] = {s.lower() for s in regex_skills_flat}
        comprehensive_extra = [
            s for s in llm_skills_flat if s.lower() not in seen_lower
        ]
        all_skills_comprehensive = regex_skills_flat + comprehensive_extra

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

    label = (
        "comprehensive (regex + LLM)"
        if col == "all_skills_comprehensive"
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
    """Print category-wise top terms based on regex taxonomy matches."""
    skills_raw_list: list[dict] = df["skills_raw"].tolist()

    print("\nBy category:")
    for category in taxonomy:
        category_counter: Counter = Counter()
        for skills_raw in skills_raw_list:
            if category in skills_raw:
                category_counter.update(skills_raw[category])
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
        print(f"  {row['job_title'] or 'N/A':<40} {row['salary_extracted']}")


def _aggregate_llm_skills(skills_llm_list: list[dict]) -> dict[str, Counter]:
    """Aggregate LLM skills by category across all jobs."""
    llm_skill_aggregates: dict[str, Counter] = {}
    for llm_skills in skills_llm_list:
        if not llm_skills:
            continue
        for category, skills in llm_skills.items():
            llm_skill_aggregates.setdefault(category, Counter()).update(skills)
    return llm_skill_aggregates


def _print_llm_skill_aggregates(llm_skill_aggregates: dict[str, Counter]) -> None:
    """Print top LLM-discovered terms per LLM category."""
    for category, counter in sorted(llm_skill_aggregates.items()):
        if not counter:
            continue
        top_terms = ", ".join(
            f"{term}({count})"
            for term, count in counter.most_common(_REPORT_TOP_LLM_SKILLS_COUNT)
        )
        print(f"  {category:<{_REPORT_LLM_CATEGORY_WIDTH}} {top_terms}")


def _taxonomy_term_set(taxonomy: dict[str, list[tuple[str, str]]]) -> set[str]:
    """Return normalized taxonomy keys (including aliases) for membership checks."""
    return {
        _taxonomy_plain_key(pattern)
        for terms in taxonomy.values()
        for _, pattern in terms
    }


def _count_missing_taxonomy_terms(
    skills_llm_list: list[dict],
    all_taxonomy_terms: set[str],
) -> Counter:
    """Count LLM terms absent from current taxonomy/alias coverage."""
    skills_missing_from_taxonomy: Counter = Counter()
    for llm_skills in skills_llm_list:
        for skills in (llm_skills or {}).values():
            for skill in skills:
                canonical = normalize_term(skill)
                taxonomy_key = _taxonomy_plain_key(skill)
                if (
                    canonical not in all_taxonomy_terms
                    and taxonomy_key not in all_taxonomy_terms
                ):
                    skills_missing_from_taxonomy[canonical] += 1
    return skills_missing_from_taxonomy


def _build_actionable_missing_terms(
    skills_missing_from_taxonomy: Counter,
    existing_candidate_canonicals: set[str],
    threshold: int,
) -> Counter:
    """Filter uncovered terms to queue-actionable terms."""
    skip_canonicals = {normalize_term(term) for term in SKIP_TERMS}
    return Counter(
        {
            canonical: count
            for canonical, count in skills_missing_from_taxonomy.items()
            if count >= threshold
            and canonical not in skip_canonicals
            and canonical not in existing_candidate_canonicals
        }
    )


def _print_missing_taxonomy_terms(
    skills_missing_from_taxonomy: Counter,
    actionable_missing_terms: Counter,
    threshold: int,
) -> None:
    """Print uncovered LLM terms and queue-actionable subset."""
    if not skills_missing_from_taxonomy:
        return

    print(
        f"\nLLM terms not covered by current taxonomy/aliases "
        f"({len(skills_missing_from_taxonomy)} terms):"
    )
    print(
        f"  Actionable for queue (threshold >= {threshold}, not SKIP_TERMS, not already queued): "
        f"{len(actionable_missing_terms)} terms"
    )

    for skill, count in skills_missing_from_taxonomy.most_common(
        _REPORT_TOP_MISSING_SKILLS_COUNT
    ):
        display_skill = re.sub(r"\\(.)", r"\1", skill)
        print(f"  {display_skill:<{_REPORT_SKILL_WIDTH}} {count} jobs")

    if actionable_missing_terms:
        print("\nTop actionable uncovered terms:")
        for skill, count in actionable_missing_terms.most_common(
            _REPORT_TOP_MISSING_SKILLS_COUNT
        ):
            display_skill = re.sub(r"\\(.)", r"\1", skill)
            print(f"  {display_skill:<{_REPORT_SKILL_WIDTH}} {count} jobs")

    print("  Note: raw uncovered count is broader than pending queue by design.")


def _print_llm_section(
    df: pd.DataFrame,
    taxonomy: dict[str, list[tuple[str, str]]],
    existing_candidate_canonicals: set[str],
    candidate_threshold: int,
) -> None:
    """Print LLM extraction aggregates and taxonomy coverage gaps."""
    if "skills_llm" not in df.columns or not df["skills_llm"].apply(bool).any():
        return

    print(f"\n{'=' * 60}")
    print("LLM-EXTRACTED SKILLS (open taxonomy)")
    print(f"{'=' * 60}")

    skills_llm_list: list[dict] = df["skills_llm"].tolist()
    llm_skill_aggregates = _aggregate_llm_skills(skills_llm_list)
    _print_llm_skill_aggregates(llm_skill_aggregates)

    all_taxonomy_terms = _taxonomy_term_set(taxonomy)
    skills_missing_from_taxonomy = _count_missing_taxonomy_terms(
        skills_llm_list,
        all_taxonomy_terms,
    )
    actionable_missing_terms = _build_actionable_missing_terms(
        skills_missing_from_taxonomy,
        existing_candidate_canonicals,
        candidate_threshold,
    )
    _print_missing_taxonomy_terms(
        skills_missing_from_taxonomy,
        actionable_missing_terms,
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
        "all_skills_flat",
        "all_skills_comprehensive",
        "skills_llm",
        "has_description",
    ]
    export_df = df.drop(columns=[col for col in internal_columns if col in df.columns])

    for category in taxonomy:
        export_df[category] = df["skills_raw"].apply(
            lambda raw, cat=category: ", ".join(raw.get(cat, []))
        )

    if "skills_llm" in df.columns and df["skills_llm"].apply(bool).any():
        llm_categories = sorted({cat for row in df["skills_llm"] if row for cat in row})
        for category in llm_categories:
            export_df[f"llm_{category}"] = df["skills_llm"].apply(
                lambda llm, cat=category: ", ".join((llm or {}).get(cat, []))
            )

    export_df.to_excel(output_path, index=False)
    print(f"\nExcel saved → {output_path.resolve()}")


# ── Entry point helpers ───────────────────────────────────────────────────────


def _resolve_input_paths(args: Namespace, data_dir: Path) -> list[Path] | None:
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


def _build_llm_client(base_url: str, model: str):
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
        help="Promote pending LLM candidates with jobs_count >= N (default 2) into taxonomy",
    )
    parser.add_argument(
        "--candidates",
        action="store_true",
        help="Show pending taxonomy candidates queue (no analysis)",
    )
    args = parser.parse_args()

    data_dir = Path(OUTPUT_DIR)
    conn = open_db(data_dir)
    init_db(conn)

    # Standalone: inspect candidates queue only
    if args.candidates:
        print_candidates(conn)
        conn.close()
        return

    # promote-only mode: no job files needed
    promote_only = args.promote is not None and not args.file and not args.all
    if promote_only:
        apply_candidates(conn, max(args.promote, 1))
        conn.close()
        return

    # Resolve which job file(s) to load
    paths = _resolve_input_paths(args, data_dir)
    if paths is None:
        # No files found; still run promote if requested
        if args.promote is not None:
            apply_candidates(conn, max(args.promote, 1))
        conn.close()
        return

    # Promote before loading taxonomy so analysis uses the enriched term set
    if args.promote is not None:
        apply_candidates(conn, max(args.promote, 1))

    taxonomy = load_taxonomy(conn)
    term_count = sum(len(terms) for terms in taxonomy.values())
    print(
        f"Taxonomy loaded: {term_count} terms (+ aliases) across {len(taxonomy)} categories"
    )

    print(f"Loading from: {[str(p) for p in paths]}")
    jobs = load_jobs(paths)
    print(f"Loaded {len(jobs)} unique jobs.")

    if not jobs:
        conn.close()
        return

    llm_client = None
    if args.llm:
        llm_client = _build_llm_client(NINEROUTER_BASE_URL, NINEROUTER_MODEL)

    candidate_threshold = 2
    existing_candidate_canonicals: set[str] = set()

    df = analyze(jobs, taxonomy, llm_client=llm_client, conn=conn)

    # After LLM extraction, auto-queue newly discovered terms as candidates
    if args.llm and llm_client:
        promote_llm_to_candidates(conn, threshold=candidate_threshold)

    existing_candidate_canonicals = {
        row["canonical"]
        for row in conn.execute("SELECT canonical FROM taxonomy_candidates")
    }

    conn.close()

    print_report(
        df,
        taxonomy,
        existing_candidate_canonicals=existing_candidate_canonicals,
        candidate_threshold=candidate_threshold,
    )

    output_stem = paths[0].stem if len(paths) == 1 else "jobs_all"
    save_excel(df, data_dir / f"{output_stem}_analysis.xlsx", taxonomy)


if __name__ == "__main__":
    main()
