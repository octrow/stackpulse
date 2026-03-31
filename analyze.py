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
from collections import Counter
from datetime import date
from pathlib import Path

import pandas as pd

# ── Seed taxonomy (used only to initialise the DB on first run) ───────────────

SKILLS_SEED: dict[str, list[str]] = {
    "Languages": [
        "python", "go", "golang", "rust", "java", "kotlin", "scala",
        "typescript", "javascript", "c\\+\\+", "c#", "ruby", "php",
        "elixir", "bash", "sql", "c",
    ],
    "Python Frameworks": [
        "fastapi", "django", "flask", "aiohttp", "starlette", "tornado",
        "litestar", "sanic", "django rest framework", "drf", "nest.js",
        "django ninja",
    ],
    "Python Libraries": [
        "sqlalchemy", "pydantic", "alembic", "celery", "pytest",
        "httpx", "aiofiles", "asyncpg", "psycopg", "boto3",
        "pandas", "numpy", "pydantic.v2", "typer", "click",
        "poetry", "uv", "pillow", "duckdb", "dlt", "delta-rs",
        "pyproject.toml",
    ],
    "Databases — Relational": [
        "postgresql", "postgres", "mysql", "mariadb", "sqlite",
        "aurora", "cockroachdb", "tidb", "orm",
    ],
    "Databases — NoSQL / Search": [
        "mongodb", "redis", "elasticsearch", "opensearch",
        "cassandra", "dynamodb", "firestore", "couchdb",
        "neo4j", "memcached", "vector database", "vector store",
    ],
    "Databases — Analytical": [
        "clickhouse", "bigquery", "snowflake", "redshift",
        "databricks", "dbt", "iceberg", "duckdb", "data lake",
    ],
    "Cloud": [
        "aws", "gcp", "google cloud", "azure",
        "lambda", "ec2", "ecs", "fargate", "s3",
        "cloud run", "app engine", "azure functions",
        "step functions", "bedrock", "api gateway", "cloudwatch", "x-ray",
    ],
    "Containers & Orchestration": [
        "kubernetes", "k8s", "docker", "helm", "argo",
        "istio", "envoy", "containerd", "eks",
    ],
    "IaC & CI/CD": [
        "terraform", "ansible", "pulumi", "cdk",
        "ci/cd", "github actions", "gitlab ci", "jenkins",
        "circleci", "argocd", "flux", "dagger",
    ],
    "Messaging & Streaming": [
        "kafka", "rabbitmq", "sqs", "sns", "pubsub",
        "nats", "activemq", "kinesis", "eventbridge",
    ],
    "API & Architecture": [
        "rest", "restful", "graphql", "grpc", "websocket",
        "openapi", "swagger", "proto", "protobuf",
        "microservices", "monolith", "serverless",
        "event.driven", "event sourcing",
        "cqrs", "domain.driven", "ddd",
        "hexagonal", "clean architecture", "solid", "dry", "iam", "auth",
    ],
    "Auth & Security": [
        "oauth", "oauth2", "openid", "oidc", "jwt",
        "saml", "keycloak", "auth0", "okta",
        "ssl", "tls", "vault", "rbac",
    ],
    "Monitoring & Observability": [
        "prometheus", "grafana", "datadog", "newrelic", "sentry",
        "opentelemetry", "jaeger", "zipkin",
        "elk", "loki", "splunk", "cloudwatch",
    ],
    "Testing": [
        "pytest", "unittest", "tdd", "bdd",
        "integration test", "unit test", "e2e",
        "testcontainers", "hypothesis", "coverage.py", "gcov",
    ],
    "AI / ML (mentioned in JD)": [
        "llm", "openai", "langchain", "langgraph", "llamaindex",
        "vector database", "pgvector", "pinecone", "weaviate",
        "rag", "fine.tun", "machine learning", "ml",
        "generative ai", "agentic", "multi-agent", "prompt engineering",
        "ai orchestration", "claude", "roo code", "cursor", "mcp",
    ],
    "Soft / Process": [
        "agile", "scrum", "kanban", "code review",
        "mentoring", "team lead", "tech lead", "staff engineer",
        "cross.functional", "stakeholder", "ownership",
    ],
    "Languages (non-technical)": [
        "english", "german", "deutsch", "french", "spanish",
        "dutch", "portuguese",
    ],
}

# ── Candidate promotion constants ─────────────────────────────────────────────

# Generic terms that should never enter the taxonomy regardless of frequency
SKIP_TERMS: set[str] = {
    "api", "testing", "automation", "debugging", "configuration",
    "scalability", "concurrency", "orchestration", "containerization",
    "profiling", "modular code", "testable code", "object-oriented programming",
    "async programming", "asynchronous programming", "error diagnosis",
    "database optimization", "memory optimization", "orm optimization",
    "restful apis", "rest apis", "ci/cd workflows", "ci/cd pipelines",
    "agentic systems", "autonomous ai agents",
}

# Maps LLM output category names → taxonomy categories (seeded into DB once)
LLM_CAT_SEED: list[tuple[str, str]] = [
    ("languages",     "Languages"),
    ("frameworks",    "Python Frameworks"),
    ("libraries",     "Python Libraries"),
    ("databases",     "Databases — Relational"),   # refined per-term by _DB_HINTS
    ("cloud_services","Cloud"),
    ("devops",        "IaC & CI/CD"),
    ("tools",         "Containers & Orchestration"),
    ("concepts",      "API & Architecture"),
]

# Keyword hints to pick the right DB sub-category when LLM says "databases"
_DB_HINTS: list[tuple[set[str], str]] = [
    ({"postgresql", "postgres", "mysql", "mariadb", "aurora", "sqlite",
      "cockroach", "tidb", "rds"},          "Databases — Relational"),
    ({"redis", "mongo", "elastic", "opensearch", "dynamo", "cassandra",
      "neo4j", "firestore", "couchdb", "nosql"},  "Databases — NoSQL / Search"),
    ({"bigquery", "snowflake", "redshift", "databricks", "clickhouse",
      "dbt", "iceberg", "duckdb", "data lake"}, "Databases — Analytical"),
]

# Initial alias seeds: (taxonomy_term, alias_text, language, alias_type)
# Only seeded when term_aliases table is empty.
# More aliases can be added via SQL (see module docstring).
ALIAS_SEED: list[tuple[str, str, str, str]] = [
    ("python", "python3",  "en", "variant"),
    ("python", "python 3", "en", "variant"),
]

# ── SQLite helpers ────────────────────────────────────────────────────────────

DB_FILENAME = "skills.db"


def open_db(data_dir: Path) -> sqlite3.Connection:
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(data_dir / DB_FILENAME)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_term(term: str) -> str:
    """Lowercase, collapse whitespace, escape regex metacharacters.

    The returned string is safe to embed in r'\\b' + term + r'\\b' patterns
    and can be stored directly in taxonomy.term or taxonomy_candidates.canonical.
    """
    t = term.strip().lower()
    t = re.sub(r"\s+", " ", t)
    return re.escape(t)


def resolve_category(llm_cat: str, term: str, cat_map: dict[str, str]) -> str:
    """Map an LLM category + term to the best-matching taxonomy category."""
    if llm_cat == "databases":
        t = term.lower()
        for keywords, taxonomy_cat in _DB_HINTS:
            if any(kw in t for kw in keywords):
                return taxonomy_cat
        return "Databases — Relational"  # fallback for unknown DB terms
    return cat_map.get(llm_cat, "API & Architecture")


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

    # Seed taxonomy
    if conn.execute("SELECT COUNT(*) FROM taxonomy").fetchone()[0] == 0:
        rows = [
            (cat, term)
            for cat, terms in SKILLS_SEED.items()
            for term in terms
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO taxonomy(category, term) VALUES(?, ?)", rows
        )
        conn.commit()
        print(f"  [DB] Seeded taxonomy with {len(rows)} terms.")

    # Seed llm_category_map
    if conn.execute("SELECT COUNT(*) FROM llm_category_map").fetchone()[0] == 0:
        conn.executemany(
            "INSERT OR IGNORE INTO llm_category_map VALUES (?,?)", LLM_CAT_SEED
        )
        conn.commit()

    # Seed term_aliases
    if conn.execute("SELECT COUNT(*) FROM term_aliases").fetchone()[0] == 0:
        for tax_term, alias_text, lang, atype in ALIAS_SEED:
            row = conn.execute(
                "SELECT id FROM taxonomy WHERE term=?", (tax_term,)
            ).fetchone()
            if row:
                canon = normalize_term(alias_text)
                conn.execute(
                    """INSERT OR IGNORE INTO term_aliases
                       (taxonomy_id, alias, canonical, lang, alias_type)
                       VALUES (?,?,?,?,?)""",
                    (row["id"], alias_text, canon, lang, atype),
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

    # Base terms
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


def _tax_key(term: str) -> str:
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
    cat_map = dict(
        conn.execute("SELECT llm_category, taxonomy_category FROM llm_category_map").fetchall()
    )

    # Taxonomy existence check uses plain-text keys (re.escape undone) so that
    # multi-word terms like "github actions" and C-style terms like c\+\+ are
    # correctly matched against plain LLM output.
    existing_tax_keys: set[str] = {
        _tax_key(row["term"])
        for row in conn.execute("SELECT term FROM taxonomy")
    }
    # Aliases are already stored as normalize_term() output — compare by canonical
    existing_alias_canonicals: set[str] = {
        row["canonical"] for row in conn.execute("SELECT canonical FROM term_aliases")
    }

    # Existing candidates keyed by canonical → current jobs_count
    existing_candidates: dict[str, int] = {
        row["canonical"]: row["jobs_count"]
        for row in conn.execute("SELECT canonical, jobs_count FROM taxonomy_candidates")
    }

    rows = conn.execute("""
        SELECT skill, category, COUNT(DISTINCT url_key) AS n
        FROM llm_results
        GROUP BY skill, category
        HAVING n >= ?
        ORDER BY n DESC
    """, (threshold,)).fetchall()

    added = updated = 0
    today = date.today().isoformat()

    for row in rows:
        skill: str = row["skill"]
        llm_cat: str = row["category"]
        count: int = row["n"]

        if skill.lower() in SKIP_TERMS:
            continue

        # Check against taxonomy using plain-text comparison
        if skill.strip().lower() in existing_tax_keys:
            continue

        canonical = normalize_term(skill)

        # Check against aliases using normalized comparison
        if canonical in existing_alias_canonicals:
            continue

        taxonomy_cat = resolve_category(llm_cat, skill, cat_map)

        if canonical in existing_candidates:
            if count > existing_candidates[canonical]:
                conn.execute(
                    "UPDATE taxonomy_candidates SET jobs_count=? WHERE canonical=?",
                    (count, canonical),
                )
                updated += 1
        else:
            conn.execute(
                """INSERT OR IGNORE INTO taxonomy_candidates
                   (term, canonical, taxonomy_category, llm_category, jobs_count, added_date)
                   VALUES (?,?,?,?,?,?)""",
                (skill, canonical, taxonomy_cat, llm_cat, count, today),
            )
            added += 1

    conn.commit()

    pending = conn.execute(
        "SELECT COUNT(*) FROM taxonomy_candidates WHERE status='pending'"
    ).fetchone()[0]
    print(
        f"  [Candidates] {added} new, {updated} updated "
        f"(threshold >=\u2009{threshold} jobs) \u2192 {pending} total pending"
    )
    return added


def apply_candidates(conn: sqlite3.Connection, min_jobs: int = 2) -> int:
    """Promote pending candidates with jobs_count >= min_jobs into taxonomy.

    Returns the number of terms promoted.
    """
    rows = conn.execute(
        """SELECT term, canonical, taxonomy_category, jobs_count
           FROM taxonomy_candidates
           WHERE status='pending' AND jobs_count >= ?
           ORDER BY jobs_count DESC""",
        (min_jobs,),
    ).fetchall()

    if not rows:
        print(f"  [Promote] No pending candidates with jobs_count >= {min_jobs}.")
        return 0

    print(f"\n  [Promote] Adding {len(rows)} terms to taxonomy (threshold >=\u2009{min_jobs} jobs):")
    print(f"  {'Term':<28} {'Category':<32} {'Jobs'}")
    print(f"  {'-'*28} {'-'*32} {'-'*4}")
    for row in rows:
        print(f"  {row['term']:<28} {row['taxonomy_category']:<32} {row['jobs_count']}")
        conn.execute(
            "INSERT OR IGNORE INTO taxonomy(category, term) VALUES (?,?)",
            (row["taxonomy_category"], row["canonical"]),
        )
        conn.execute(
            "UPDATE taxonomy_candidates SET status='approved' WHERE canonical=?",
            (row["canonical"],),
        )

    conn.commit()
    return len(rows)


def print_candidates(conn: sqlite3.Connection) -> None:
    """Print the taxonomy_candidates queue."""
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

    status_counts: Counter = Counter(r["status"] for r in rows)
    print(
        f"\nTaxonomy candidates: {len(rows)} total  "
        f"({status_counts.get('pending', 0)} pending, "
        f"{status_counts.get('approved', 0)} approved, "
        f"{status_counts.get('rejected', 0)} rejected)"
    )

    pending = [r for r in rows if r["status"] == "pending"]
    if pending:
        print(f"\n  {'Term':<25} {'Category':<30} {'LLM Cat':<15} {'Jobs'}")
        print(f"  {'-'*25} {'-'*30} {'-'*15} {'-'*4}")
        for r in pending:
            print(
                f"  {r['term']:<25} {r['taxonomy_category']:<30} "
                f"{r['llm_category']:<15} {r['jobs_count']}"
            )

    print(f"\nTo promote all pending (>= 2 jobs):   py analyze.py --promote")
    print(f"To reject a term:  sqlite3 data/skills.db "
          f"\"UPDATE taxonomy_candidates SET status='rejected' WHERE canonical='<term>'\"")


# ── LLM extraction ────────────────────────────────────────────────────────────

NINEROUTER_BASE = "http://localhost:20128/v1"
NINEROUTER_MODEL = "groq/llama-3.3-70b-versatile"
# Optional fallback model when the primary hits a daily quota (9router combo or another provider).
# Set to e.g. "gc/gemini-2.5-pro" or a 9router combo name. Leave empty to skip on exhaustion.
NINEROUTER_FALLBACK_MODEL = "9router-combo"
# Max seconds to sleep-and-retry on a 429.  Longer waits are skipped (not worth blocking).
MAX_429_WAIT_S = 30

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
    return hashlib.md5(url.encode()).hexdigest()


def _llm_cache_get(conn: sqlite3.Connection, url_key: str) -> dict[str, list[str]] | None:
    rows = conn.execute(
        "SELECT category, skill FROM llm_results WHERE url_key = ?", (url_key,)
    ).fetchall()
    if not rows:
        return None
    result: dict[str, list[str]] = {}
    for row in rows:
        result.setdefault(row["category"], []).append(row["skill"])
    return result


def _llm_cache_set(
    conn: sqlite3.Connection, url: str, url_key: str, result: dict[str, list[str]]
) -> None:
    rows = [
        (url_key, url, cat, skill)
        for cat, skills in result.items()
        for skill in skills
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO llm_results(url_key, url, category, skill) VALUES(?,?,?,?)",
        rows,
    )
    conn.commit()


def _parse_retry_after(error_msg: str) -> int | None:
    """Parse the suggested wait time (seconds) from a 429 error message.

    Handles both:
      "Please try again in 18m0.864s"  (groq TPD exhaustion)
      "reset after 1m 4s"              (per-minute window reset)
    Returns seconds rounded up, or None if unparseable.
    """
    for pattern in (
        r"try again in (?:(\d+)m\s*)?(\d+(?:\.\d+)?)s",
        r"reset after (?:(\d+)m\s*)?(\d+(?:\.\d+)?)s",
    ):
        m = re.search(pattern, str(error_msg))
        if m:
            minutes = int(m.group(1)) if m.group(1) else 0
            seconds = float(m.group(2))
            return int(minutes * 60 + seconds) + 2  # +2 s buffer
    return None


def _llm_call(client, model: str, text: str) -> dict[str, list[str]]:
    """Single LLM call; raises on error."""
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": LLM_PROMPT + text[:6000]}],
        max_tokens=800,
        temperature=0,
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def extract_skills_llm(
    text: str, url: str, conn: sqlite3.Connection, client
) -> dict[str, list[str]]:
    """Call LLM via 9router to extract skills. Uses DB cache to avoid re-calls.

    On 429:
      - If the suggested wait is ≤ MAX_429_WAIT_S, sleeps and retries once.
      - If a NINEROUTER_FALLBACK_MODEL is configured, tries that next.
      - Otherwise logs a warning and returns {}.
    """
    from openai import RateLimitError

    key = _url_key(url)
    cached = _llm_cache_get(conn, key)
    if cached is not None:
        return cached

    def _try_call(model: str, attempt_label: str) -> dict[str, list[str]] | None:
        try:
            return _llm_call(client, model, text)
        except RateLimitError as e:
            wait = _parse_retry_after(str(e))
            if wait is not None and wait <= MAX_429_WAIT_S:
                print(
                    f"  [LLM] 429 on {attempt_label} — sleeping {wait}s then retrying …"
                )
                try:
                    time.sleep(wait)
                except KeyboardInterrupt:
                    raise
                try:
                    return _llm_call(client, model, text)
                except RateLimitError as e2:
                    print(f"  [LLM] 429 again after retry ({attempt_label}): {e2}")
                    return None
            else:
                wait_str = f"{wait}s" if wait else "unknown"
                print(
                    f"  [LLM] 429 on {attempt_label} — wait {wait_str} exceeds limit, "
                    f"skipping primary"
                )
                return None
        except Exception as e:
            print(f"  [LLM] Warning: extraction failed for {url[:60]}: {e}")
            return None

    result = _try_call(NINEROUTER_MODEL, NINEROUTER_MODEL)

    if result is None and NINEROUTER_FALLBACK_MODEL:
        print(f"  [LLM] Trying fallback model: {NINEROUTER_FALLBACK_MODEL}")
        result = _try_call(NINEROUTER_FALLBACK_MODEL, f"fallback:{NINEROUTER_FALLBACK_MODEL}")

    if result is None:
        return {}

    _llm_cache_set(conn, url, key, result)
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
    found: dict[str, list[str]] = {}
    for category, term_pairs in taxonomy.items():
        seen: set[str] = set()
        hits: list[str] = []
        for display, pattern in term_pairs:
            if display not in seen and re.search(r"\b" + pattern + r"\b", text_lower):
                seen.add(display)
                hits.append(display)
        if hits:
            found[category] = hits
    return found


# ── Data loading ──────────────────────────────────────────────────────────────


def load_jobs(paths: list[Path]) -> list[dict]:
    jobs = []
    for p in paths:
        with open(p) as f:
            jobs.extend(json.load(f))
    seen: set[str] = set()
    unique = []
    for j in jobs:
        url = j.get("linkedin_url", "")
        if url not in seen:
            seen.add(url)
            unique.append(j)
    return unique


# ── Analysis ──────────────────────────────────────────────────────────────────


def analyze(
    jobs: list[dict],
    taxonomy: dict[str, list[tuple[str, str]]],
    llm_client=None,
    conn: sqlite3.Connection | None = None,
) -> pd.DataFrame:
    rows = []
    for job in jobs:
        desc = job.get("job_description") or ""
        title = job.get("job_title") or ""
        combined = f"{title} {desc}"
        url = job.get("linkedin_url", "")

        skills_found = extract_skills(combined, taxonomy)

        llm_skills: dict[str, list[str]] = {}
        if llm_client and conn is not None:
            llm_skills = extract_skills_llm(combined, url, conn, llm_client)

        rows.append({
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
            "all_skills_flat": [s for hits in skills_found.values() for s in hits],
            "skills_llm": llm_skills,
        })

    return pd.DataFrame(rows)


def print_report(
    df: pd.DataFrame, taxonomy: dict[str, list[tuple[str, str]]]
) -> None:
    print(f"\n{'=' * 60}")
    print(f"SKILLS ANALYSIS — {len(df)} unique job postings")
    print(f"{'=' * 60}")

    all_skills: Counter = Counter()
    for skills_list in df["all_skills_flat"]:
        all_skills.update(skills_list)

    print("\nTop 30 skills mentioned across all postings:")
    for skill, count in all_skills.most_common(30):
        pct = count / len(df) * 100
        bar = "█" * int(pct / 2)
        print(f"  {skill:<25} {count:>4} jobs  ({pct:>5.1f}%)  {bar}")

    print("\nBy category:")
    for category in taxonomy:
        cat_counter: Counter = Counter()
        for _, row in df.iterrows():
            raw = row["skills_raw"]
            if category in raw:
                cat_counter.update(raw[category])
        if cat_counter:
            top = ", ".join(f"{t}({c})" for t, c in cat_counter.most_common(8))
            print(f"  {category:<30} {top}")

    print("\nTop locations in results:")
    for loc, cnt in df["location"].value_counts().head(15).items():
        print(f"  {loc:<35} {cnt}")

    salary_found = df[df["salary_extracted"].notna()]
    print(f"\nSalary hints found in {len(salary_found)}/{len(df)} postings:")
    for _, row in salary_found.head(20).iterrows():
        print(f"  {row['job_title'] or 'N/A':<40} {row['salary_extracted']}")

    if "skills_llm" in df.columns and df["skills_llm"].apply(bool).any():
        print(f"\n{'=' * 60}")
        print("LLM-EXTRACTED SKILLS (open taxonomy)")
        print(f"{'=' * 60}")

        llm_agg: dict[str, Counter] = {}
        for _, row in df.iterrows():
            llm = row.get("skills_llm") or {}
            for cat, skills in llm.items():
                llm_agg.setdefault(cat, Counter()).update(skills)

        for cat, counter in sorted(llm_agg.items()):
            if counter:
                top = ", ".join(f"{t}({c})" for t, c in counter.most_common(15))
                print(f"  {cat:<20} {top}")

        # All canonical display terms currently in taxonomy
        all_taxonomy_terms = {
            display.lower()
            for terms in taxonomy.values()
            for display, _ in terms
        }
        gap: Counter = Counter()
        for _, row in df.iterrows():
            for skills in (row.get("skills_llm") or {}).values():
                for s in skills:
                    if s.lower() not in all_taxonomy_terms:
                        gap[s.lower()] += 1

        if gap:
            print(f"\nSkills found by LLM but MISSING from taxonomy ({len(gap)} terms):")
            for skill, count in gap.most_common(50):
                print(f"  {skill:<30} {count} jobs")


def save_excel(
    df: pd.DataFrame,
    output_path: Path,
    taxonomy: dict[str, list[tuple[str, str]]],
) -> None:
    drop_cols = ["skills_raw", "all_skills_flat", "skills_llm"]
    export_df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    for category in taxonomy:
        export_df[category] = df["skills_raw"].apply(
            lambda raw, cat=category: ", ".join(raw.get(cat, []))
        )

    if "skills_llm" in df.columns and df["skills_llm"].apply(bool).any():
        llm_cats = sorted({cat for row in df["skills_llm"] if row for cat in row})
        for cat in llm_cats:
            export_df[f"llm_{cat}"] = df["skills_llm"].apply(
                lambda llm, c=cat: ", ".join((llm or {}).get(c, []))
            )

    export_df.to_excel(output_path, index=False)
    print(f"\nExcel saved → {output_path.resolve()}")


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze scraped LinkedIn jobs")
    parser.add_argument("--file", type=str, help="Specific JSON file to analyze")
    parser.add_argument("--all", action="store_true", help="Merge all JSON files in data/")
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

    data_dir = Path("data")
    conn = open_db(data_dir)
    init_db(conn)

    # Standalone: inspect candidates queue
    if args.candidates:
        print_candidates(conn)
        conn.close()
        return

    # Determine whether we need job files
    promote_only = args.promote is not None and not args.file and not args.all

    paths: list[Path] = []
    if not promote_only:
        if args.file:
            paths = [Path(args.file)]
        elif args.all:
            paths = sorted(data_dir.glob("jobs_*.json"))
            if not paths:
                print("No job files found in data/. Run scrape.py first.")
                if args.promote is not None:
                    apply_candidates(conn, max(args.promote, 1))
                conn.close()
                return
        else:
            today = date.today().isoformat()
            default = data_dir / f"jobs_{today}.json"
            if not default.exists():
                file_candidates = sorted(data_dir.glob("jobs_*.json"))
                if not file_candidates:
                    print("No job files found. Run scrape.py first.")
                    if args.promote is not None:
                        apply_candidates(conn, max(args.promote, 1))
                    conn.close()
                    return
                default = file_candidates[-1]
                print(f"Today's file not found, using latest: {default}")
            paths = [default]

    # Promote before loading taxonomy so analysis uses enriched terms
    if args.promote is not None:
        apply_candidates(conn, max(args.promote, 1))

    if promote_only:
        conn.close()
        return

    taxonomy = load_taxonomy(conn)
    term_count = sum(len(v) for v in taxonomy.values())
    print(f"Taxonomy loaded: {term_count} terms (+ aliases) across {len(taxonomy)} categories")

    print(f"Loading from: {[str(p) for p in paths]}")
    jobs = load_jobs(paths)
    print(f"Loaded {len(jobs)} unique jobs.")

    if not jobs:
        conn.close()
        return

    llm_client = None
    if args.llm:
        try:
            from openai import OpenAI
            llm_client = OpenAI(base_url=NINEROUTER_BASE, api_key="local")
            print(f"LLM extraction enabled → {NINEROUTER_MODEL} via 9router")
        except ImportError:
            print("openai package not installed. Run: pip install openai")

    df = analyze(jobs, taxonomy, llm_client=llm_client, conn=conn)

    # After LLM extraction, auto-queue newly discovered terms as candidates
    if args.llm and llm_client:
        promote_llm_to_candidates(conn, threshold=2)

    conn.close()

    print_report(df, taxonomy)

    stem = paths[0].stem if len(paths) == 1 else "jobs_all"
    save_excel(df, data_dir / f"{stem}_analysis.xlsx", taxonomy)


if __name__ == "__main__":
    main()
