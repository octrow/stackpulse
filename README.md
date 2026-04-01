# stackpulse

Collects job postings across Europe, saves them as JSON, and analyzes the skill landscape to answer: _"what do employers
actually want in 2026?"_

Currently scrapes LinkedIn via Playwright, using a custom job-page scraper that fixes the library's broken content
extraction. Built to be source-agnostic — LinkedIn is just the first feed.

---

## Project layout

```text
stackpulse/
├── config.py               # search queries, delays, timeouts, paths, and LLM settings
├── cli.py                  # Typer + Rich CLI (stackpulse command)
├── setup_session.py        # one-time LinkedIn login → session file
├── scrape.py               # main scraper orchestrator + per-query/per-job helpers
├── job_scraper_direct.py   # custom Playwright scraper (replaces broken library JobScraper)
├── analyze.py              # skill analysis + LLM retry/fallback helpers + Excel export
├── pyproject.toml          # packaging + console script entrypoint
├── requirements.txt
├── .env                    # LinkedIn credentials (gitignored)
├── .env.example
└── data/
    ├── jobs_YYYY-MM-DD.json        # one file per scrape day
    ├── jobs_*_analysis.xlsx        # Excel export with one column per skill category
    ├── skills.db                   # SQLite: taxonomy + LLM extraction cache
    ├── scraper.log                 # timestamped run log
    └── debug/                      # screenshots + HTML dumped when a page fails to load
```

---

## Setup

```bash
pip install -r requirements.txt
playwright install chromium

cp .env.example .env
# fill in LINKEDIN_EMAIL and LINKEDIN_PASSWORD
```

(Optional, for installable `stackpulse` command):

```bash
pip install -e .
```

---

## CLI (Typer + Rich)

Running `stackpulse` with no arguments launches an **interactive wizard** that prompts for command and options:

```
StackPulse — what would you like to do?

  1  analyze        Analyze scraped jobs and export stats
  2  scrape         Scrape LinkedIn for new jobs
  3  auto           Bootstrap + scrape + analyze end-to-end
  4  setup-session  Create or refresh LinkedIn session
  5  quit

Choice [1-5]:
```

All subcommands still work non-interactively via flags:

```bash
stackpulse --help
stackpulse setup-session
stackpulse scrape --limit 3
stackpulse analyze --all --llm
stackpulse analyze --all --title-contains "Backend" --location-contains "Berlin"
stackpulse analyze --candidates
```

### Auto workflow (one command)

```bash
stackpulse auto
stackpulse auto --limit 3 --all
stackpulse auto --llm --promote 2
```

`stackpulse auto` behavior:

- Creates virtualenv if missing (`.venv` by default)
- Installs dependencies only when missing
- Installs Playwright Chromium only when missing
- Skips session setup if `session.json` already exists
- Fails fast on the first failed step and prints a Rich summary table

You can still run legacy script entrypoints (`py setup_session.py`, `py scrape.py`, `py analyze.py`).

### Shell completion

Requires `pip install -e .` first (adds `stackpulse` to `$PATH`).

```bash
stackpulse --install-completion   # install tab completion for your current shell
stackpulse --show-completion      # print the completion script (for manual setup)
```

Restart your shell (or `source ~/.zshrc` / `~/.bashrc`) after installing.

---

## Workflow

### 1. Create a session (once, or when session expires)

```bash
py setup_session.py
# or
stackpulse setup-session
```

- If credentials are in `.env`, logs in programmatically
- Otherwise opens a browser window for manual login
- Saves cookies/storage to `SESSION_FILE` from `config.py` (default `session.json`)
- Re-run whenever LinkedIn shows you a login page again

### 2. Scrape jobs

```bash
py scrape.py              # full run — all queries in config.py
py scrape.py --limit 3    # quick test, 3 jobs per query
py scrape.py --fresh      # ignore all previous results, re-scrape everything

# Typer CLI equivalents
stackpulse scrape
stackpulse scrape --limit 3
stackpulse scrape --fresh
```

**Resume is automatic** — on every run the scraper loads all previously collected URLs from every `data/jobs_*.json`
file and skips them. No flag needed.

**Ctrl+C exits cleanly** — progress is saved after every job. Re-run at any time to pick up where you left off.

Output is saved incrementally after each job to `data/jobs_YYYY-MM-DD.json`.

### 3. Analyze skills

```bash
py analyze.py                              # analyze today's file
py analyze.py --file data/jobs_2026-04-01.json
py analyze.py --all                        # merge all collected files
py analyze.py --llm                        # + open LLM extraction (free, via 9router)
py analyze.py --all --llm

# Typer CLI equivalents
stackpulse analyze
stackpulse analyze --file data/jobs_2026-04-01.json
stackpulse analyze --all
stackpulse analyze --llm          # note: double-dash required; -llm and "analyze llm" are invalid
stackpulse analyze --all --llm

# Cohort filters (case-insensitive substring match)
stackpulse analyze --all --title-contains "Backend"
stackpulse analyze --all --location-contains "Berlin"
stackpulse analyze --all --llm --title-contains "Senior" --location-contains "Germany"
```

Prints a skill frequency table to stdout and saves `data/jobs_*_analysis.xlsx` with one column per skill category.

**Report sections:**

| Section            | Description                                                                       |
|--------------------|-----------------------------------------------------------------------------------|
| Extraction quality | Jobs with empty description and jobs with zero skills extracted (%)               |
| Top skills         | Frequency + prevalence % + bar; uses merged regex+LLM metric when `--llm` was run |
| By category        | Top terms per taxonomy category with prevalence % (regex + LLM unified)           |
| Top locations      | Most frequent scraped `location` values                                           |
| Skills by location | Top 3 skills per `search_location` (only shown when >1 search location present)   |
| Salary hints       | Postings where a salary pattern was regex-extracted (with company + location)     |
| Coverage gaps      | Taxonomy terms discovered by LLM but not yet in taxonomy (only with `--llm`)      |

`--llm` mode calls `NINEROUTER_MODEL` through your local 9router endpoint (`NINEROUTER_BASE_URL`, default
`http://localhost:20128/v1`) with a **taxonomy-aware prompt** — the full taxonomy is sent to the LLM so it matches
against known terms first and only flags genuinely new discoveries. Results are cached in `data/skills.db` — repeat
runs are instant with no API calls.

After each `--llm` run, newly discovered terms (seen in ≥ `LLM_CANDIDATE_THRESHOLD` jobs, default 2) are automatically
queued in `taxonomy_candidates`. Because the LLM is taxonomy-aware, uncovered terms are genuinely new
technologies/tools — not synonyms or generic concepts.

`--llm` prints two gap metrics: raw uncovered terms and actionable uncovered terms. Actionable terms satisfy
`jobs_count >= threshold`, are not in `SKIP_TERMS`, and are not already present in `taxonomy_candidates`.

**Rate-limit handling (429):** `analyze.py` parses retry wait from the provider error. If wait ≤
`LLM_RATE_LIMIT_MAX_WAIT_SECONDS` (default `30`), it sleeps and retries once. For longer waits (daily quota exhausted),
it falls back to `NINEROUTER_FALLBACK_MODEL` if configured.

### 4. Promote LLM-discovered skills into taxonomy

```bash
py analyze.py --candidates                 # inspect the promotion queue (all statuses + pending)
py analyze.py --promote                    # promote pending terms (≥2 jobs) into taxonomy
py analyze.py --promote 3                  # same, threshold = 3 jobs
py analyze.py --all --promote              # promote first, then analyze with enriched taxonomy

# Typer CLI equivalents
stackpulse analyze --candidates
stackpulse analyze --promote 2
stackpulse analyze --promote 3
stackpulse analyze --all --promote 2
```

Once promoted, terms are matched by regex in all future runs — **no `--llm` flag needed**.

To reject a term so it never reappears:

```bash
sqlite3 data/skills.db "UPDATE taxonomy_candidates SET status='rejected' WHERE canonical='<term>'"
```

---

## Configuration (`config.py`)

### Core scrape settings

| Variable                | Default        | Description                                          |
|-------------------------|----------------|------------------------------------------------------|
| `SEARCH_QUERIES`        | 11 queries     | List of `(keywords, location)` tuples                |
| `JOBS_PER_QUERY`        | `25`           | Max jobs fetched per query                           |
| `DELAY_BETWEEN_JOBS`    | `3`            | Pause (seconds) between individual job page scrapes  |
| `DELAY_BETWEEN_QUERIES` | `5`            | Pause (seconds) between search queries               |
| `OUTPUT_DIR`            | "data"         | Directory for JSON output, logs, DB, and debug dumps |
| `SESSION_FILE`          | "session.json" | Saved browser session                                |

### Scraper timeouts / extraction behavior

| Variable                     | Default | Description                                                 |
|------------------------------|---------|-------------------------------------------------------------|
| `PAGE_LOAD_TIMEOUT_MS`       | `60000` | Timeout for `page.goto(..., wait_until="domcontentloaded")` |
| `H1_WAIT_TIMEOUT_MS`         | `15000` | Timeout waiting for job title `<h1>`                        |
| `BUTTON_CLICK_TIMEOUT_MS`    | `3000`  | Timeout for clicking expand buttons                         |
| `POST_CLICK_SETTLE_SECONDS`  | `0.5`   | Sleep after each expand click                               |
| `POST_EXPAND_SETTLE_SECONDS` | `1.0`   | Sleep before extraction starts                              |
| `DEBUG_HTML_SNIPPET_CHARS`   | `8000`  | Max HTML chars written to debug file                        |
| `DESCRIPTION_MIN_CHARS`      | `100`   | Minimum length for accepted job description                 |

### LLM / analysis settings

| Variable                          | Default                        | Description                                        |
|-----------------------------------|--------------------------------|----------------------------------------------------|
| `DB_FILENAME`                     | "skills.db"                    | SQLite filename inside `OUTPUT_DIR`                |
| `NINEROUTER_BASE_URL`             | "http://localhost:20128/v1"    | OpenAI-compatible 9router endpoint                 |
| `NINEROUTER_MODEL`                | "groq/llama-3.3-70b-versatile" | Primary extraction model                           |
| `NINEROUTER_FALLBACK_MODEL`       | "9router-combo"                | Fallback model/combo when primary is quota-limited |
| `LLM_MAX_INPUT_CHARS`             | `8000`                         | Max characters sent from each posting to LLM       |
| `LLM_MAX_OUTPUT_TOKENS`           | `1000`                         | LLM completion token cap                           |
| `LLM_RATE_LIMIT_MAX_WAIT_SECONDS` | `30`                           | Max retry sleep for 429 before fallback            |
| `RETRY_AFTER_BUFFER_SECONDS`      | `2`                            | Safety buffer added to parsed retry-after          |
| `LLM_CANDIDATE_THRESHOLD`         | `2`                            | Min job occurrences to promote a candidate term    |

Current search targets: Berlin, Hamburg, Munich, Germany (general), Vienna, Amsterdam, Luxembourg, Barcelona, Madrid,
London, Remote — all for senior Python/FastAPI backend roles.

### Recommended minimal config profile

Use these as a practical baseline in `config.py`:

```python
# Fast local test profile (quick feedback)
JOBS_PER_QUERY = 3
DELAY_BETWEEN_JOBS = 1
DELAY_BETWEEN_QUERIES = 2
PAGE_LOAD_TIMEOUT_MS = 45_000
H1_WAIT_TIMEOUT_MS = 12_000
LLM_RATE_LIMIT_MAX_WAIT_SECONDS = 15
```

```python
# Stable full-run profile (fewer limits/blocks)
JOBS_PER_QUERY = 25
DELAY_BETWEEN_JOBS = 3
DELAY_BETWEEN_QUERIES = 5
PAGE_LOAD_TIMEOUT_MS = 60_000
H1_WAIT_TIMEOUT_MS = 15_000
LLM_RATE_LIMIT_MAX_WAIT_SECONDS = 30
NINEROUTER_FALLBACK_MODEL = "9router-combo"
```

Tip: use the fast profile for selector/debug iteration, then switch back to the stable profile for production collection
runs.

---

## Collected fields

| Field                  | Source                               |
|------------------------|--------------------------------------|
| `linkedin_url`         | Job URL                              |
| `job_title`            | Page `<h1>`                          |
| `company`              | Company link near title              |
| `company_linkedin_url` | Company `/company/` href             |
| `location`             | Location text in top card            |
| `posted_date`          | "X days ago" text                    |
| `applicant_count`      | "N applicants" text                  |
| `job_description`      | Full description text                |
| `salary_extracted`     | Regex over description (best-effort) |
| `search_keywords`      | Which query found this job           |
| `search_location`      | Which location was searched          |
| `scraped_date`         | ISO date of the scrape               |

> LinkedIn does not expose salary as a structured field. `salary_extracted` is regex-based and best effort.

---

## Skill taxonomy (`data/skills.db`)

17 categories, 227+ terms, stored in `data/skills.db` (SQLite). The DB is auto-created and seeded from `SKILLS_SEED` in
`analyze.py` on first run. Additional terms accumulate automatically via the LLM promotion pipeline.

| Category                   | Examples                                                 |
|----------------------------|----------------------------------------------------------|
| Languages                  | python, go, rust, java, kotlin, typescript               |
| Python Frameworks          | fastapi, django, flask, aiohttp, starlette               |
| Python Libraries           | sqlalchemy, pydantic, celery, asyncpg, boto3             |
| Databases — Relational     | postgresql, mysql, cockroachdb, aurora                   |
| Databases — NoSQL/Search   | mongodb, redis, elasticsearch, cassandra, dynamodb       |
| Databases — Analytical     | clickhouse, bigquery, snowflake, dbt                     |
| Cloud                      | aws, gcp, azure, lambda, s3, step functions, bedrock     |
| Containers & Orchestration | kubernetes, docker, helm, argo, istio                    |
| IaC & CI/CD                | terraform, pulumi, github actions, gitlab ci, argocd     |
| Messaging & Streaming      | kafka, rabbitmq, sqs, kinesis, nats                      |
| API & Architecture         | rest, graphql, grpc, microservices, solid, cqrs, ddd     |
| Auth & Security            | oauth2, jwt, keycloak, auth0, vault                      |
| Monitoring & Observability | prometheus, grafana, datadog, opentelemetry              |
| Testing                    | pytest, tdd, testcontainers, hypothesis, coverage.py     |
| AI / ML (in JD)            | ai, llm, langchain, pgvector, rag, generative ai, cursor |
| Soft / Process             | agile, mentoring, tech lead, staff engineer              |
| Languages (non-technical)  | english, german, french, dutch                           |

Add term without code change:

```bash
sqlite3 data/skills.db "INSERT OR IGNORE INTO taxonomy(category,term) VALUES('Cloud','hetzner')"
```

Add alias (e.g. multilingual synonym):

```bash
sqlite3 data/skills.db \
  "INSERT INTO term_aliases(taxonomy_id,alias,canonical,lang,alias_type)
   SELECT id,'Node.js','node\.js','en','variant' FROM taxonomy WHERE term='node\.js'"
```

Top LLM-discovered skills across all jobs:

```bash
sqlite3 data/skills.db "SELECT skill, COUNT(DISTINCT url_key) jobs FROM llm_results GROUP BY skill ORDER BY jobs DESC LIMIT 20"
```

Promotion queue view:

```bash
sqlite3 data/skills.db "SELECT term, taxonomy_category, jobs_count, status FROM taxonomy_candidates ORDER BY jobs_count DESC"
```

---

## Known issues & limitations

- **LinkedIn SPA timing:** library `JobScraper` is unreliable for React-rendered content. `job_scraper_direct.py` waits
  for `<h1>` and uses selector fallbacks.
- **No structured salary data:** salary extraction is regex over free text.
- **Session expiry:** LinkedIn sessions expire; rerun `setup_session.py`.
- **LinkedIn UI drift:** CSS selectors can break due to A/B tests. If fields become `null`, inspect `data/debug/`
  snapshots and update selector lists.
- **LLM quota limits:** long quota windows skip sleep-retry and use fallback model if configured.
- **Single-letter language matching:** `c` is matched as `\bc\b` which can false-positive on the English word. The
  taxonomy pattern system uses `_unescape()` to derive display names, so complex regex patterns cannot be stored.
  LLM extraction correctly disambiguates C language from prose.
- **Best-effort extraction paths:** scraper field extractors fail soft by design and continue fallback traversal; debug
  logs now include selector/button context to make drift diagnosis faster.
