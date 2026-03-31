# stackpulse

Collects job postings across Europe, saves them as JSON, and analyzes the skill landscape to answer: _"what do employers actually want in 2026?"_

Currently scrapes LinkedIn via Playwright, using a custom job-page scraper that fixes the library's broken content extraction. Built to be source-agnostic — LinkedIn is just the first feed.

---

## Project layout

```
stackpulse/
├── config.py               # search queries, delays, paths — edit this
├── setup_session.py        # one-time LinkedIn login → session.json
├── scrape.py               # main scraper loop
├── job_scraper_direct.py   # custom Playwright scraper (replaces broken library JobScraper)
├── analyze.py              # skill frequency analysis + Excel export
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

---

## Workflow

### 1. Create a session (once, or when session expires)

```bash
py setup_session.py
```

- If credentials are in `.env`, logs in programmatically
- Otherwise opens a browser window for manual login
- Saves cookies/storage to `session.json`
- Re-run whenever LinkedIn shows you a login page again

### 2. Scrape jobs

```bash
py scrape.py              # full run — all queries in config.py
py scrape.py --limit 3    # quick test, 3 jobs per query
py scrape.py --fresh      # ignore all previous results, re-scrape everything
```

**Resume is automatic** — on every run the scraper loads all previously collected URLs from every `data/jobs_*.json` file and skips them. No flag needed. You can `Ctrl+C` at any time and re-run safely.

Output is saved incrementally after each job to `data/jobs_YYYY-MM-DD.json`.

### 3. Analyze skills

```bash
py analyze.py                              # analyze today's file
py analyze.py --file data/jobs_2026-04-01.json
py analyze.py --all                        # merge all collected files
py analyze.py --llm                        # + open LLM extraction (free, via 9router)
py analyze.py --all --llm
```

Prints a skill frequency table to stdout and saves `data/jobs_*_analysis.xlsx` with one column per skill category.

**`--llm` mode** calls `groq/llama-3.3-70b-versatile` through a local [9router](https://github.com/decolua/9router) instance (`localhost:20128`) to extract skills that fall outside the fixed taxonomy. Results are cached permanently in `data/skills.db` — repeat runs are instant with no API calls.

After each `--llm` run, newly discovered terms (seen in ≥ 2 jobs) are automatically queued in `taxonomy_candidates`.

**Rate-limit handling**: on a 429 the scraper parses the suggested wait time from the error message and sleeps+retries automatically if the wait is ≤ 2 minutes. For longer waits (daily quota exhausted) it falls back to `NINEROUTER_FALLBACK_MODEL` in `analyze.py`. Set this to a 9router combo to chain through multiple providers without interruption. Recommended combo order: Cerebras/Llama-3.3-70B → Cerebras/GPT-OSS-120B → Groq/Llama-4-Maverick → Together/Llama-3.3-70B-Turbo → Fireworks/Llama-3.3-70B → Gemini-cli/Gemini-3-Flash → Kiro/Claude-Haiku-4.5.

### 4. Promote LLM-discovered skills into taxonomy

```bash
py analyze.py --candidates                 # inspect the promotion queue
py analyze.py --promote                    # promote pending terms (≥2 jobs) into taxonomy
py analyze.py --promote 3                  # same, threshold = 3 jobs
py analyze.py --all --promote              # promote first, then analyze with enriched taxonomy
```

Once promoted, terms are matched by regex in all future runs — **no `--llm` flag needed**. To reject a term so it never reappears:

```bash
sqlite3 data/skills.db "UPDATE taxonomy_candidates SET status='rejected' WHERE canonical='<term>'"
```

---

## Configuration (`config.py`)

| Variable | Default | Description |
|---|---|---|
| `SEARCH_QUERIES` | 11 queries | List of `(keywords, location)` tuples |
| `JOBS_PER_QUERY` | 25 | Max jobs fetched per query (LinkedIn caps ~100) |
| `DELAY_BETWEEN_JOBS` | 3s | Pause between individual job page scrapes |
| `DELAY_BETWEEN_QUERIES` | 5s | Pause between search queries |
| `OUTPUT_DIR` | `data/` | Directory for JSON output and logs |
| `SESSION_FILE` | `session.json` | Saved browser session |

Current search targets: Berlin, Hamburg, Munich, Germany (general), Vienna, Amsterdam, Luxembourg, Barcelona, Madrid, London, Remote — all for Senior Python/FastAPI Backend Developer roles.

---

## Collected fields

| Field | Source |
|---|---|
| `linkedin_url` | Job URL |
| `job_title` | Page `<h1>` |
| `company` | Company link near title |
| `company_linkedin_url` | Company `/company/` href |
| `location` | Location span in job card |
| `posted_date` | "X days ago" text |
| `applicant_count` | "N applicants" text |
| `job_description` | Full text of the job description section |
| `salary_extracted` | Regex over description (best-effort) |
| `search_keywords` | Which query found this job |
| `search_location` | Which location was searched |
| `scraped_date` | ISO date of the scrape |

> LinkedIn does not expose salary as a structured field. `salary_extracted` is a regex over the description text — it catches ranges like `€70,000–90,000` or `80k–100k EUR` when present.

---

## Skill taxonomy (`data/skills.db`)

17 categories, 227+ terms, stored in `data/skills.db` (SQLite). The DB is auto-created and seeded from `SKILLS_SEED` in `analyze.py` on first run. Additional terms accumulate automatically via the LLM promotion pipeline.

| Category | Examples |
|---|---|
| Languages | python, go, rust, java, kotlin, typescript |
| Python Frameworks | fastapi, django, flask, aiohttp, starlette |
| Python Libraries | sqlalchemy, pydantic, celery, asyncpg, boto3 |
| Databases — Relational | postgresql, mysql, cockroachdb, aurora |
| Databases — NoSQL/Search | mongodb, redis, elasticsearch, cassandra, dynamodb |
| Databases — Analytical | clickhouse, bigquery, snowflake, dbt |
| Cloud | aws, gcp, azure, lambda, s3, step functions, bedrock |
| Containers & Orchestration | kubernetes, docker, helm, argo, istio |
| IaC & CI/CD | terraform, pulumi, github actions, gitlab ci, argocd |
| Messaging & Streaming | kafka, rabbitmq, sqs, kinesis, nats |
| API & Architecture | rest, graphql, grpc, microservices, cqrs, ddd |
| Auth & Security | oauth2, jwt, keycloak, auth0, vault |
| Monitoring & Observability | prometheus, grafana, datadog, opentelemetry |
| Testing | pytest, tdd, testcontainers, hypothesis, coverage.py |
| AI / ML (in JD) | llm, langchain, pgvector, rag, generative ai, cursor |
| Soft / Process | agile, mentoring, tech lead, staff engineer |
| Languages (non-technical) | english, german, french, dutch |

**Adding a term without editing code:**
```bash
sqlite3 data/skills.db "INSERT OR IGNORE INTO taxonomy(category,term) VALUES('Cloud','hetzner')"
```

**Adding a multilingual alias (e.g. German synonym):**
```bash
sqlite3 data/skills.db \
  "INSERT INTO term_aliases(taxonomy_id,alias,canonical,lang,alias_type)
   SELECT id,'Node.js','node\.js','en','variant' FROM taxonomy WHERE term='node\.js'"
```

**Querying LLM-discovered skills across all jobs:**
```bash
sqlite3 data/skills.db "SELECT skill, COUNT(DISTINCT url_key) jobs FROM llm_results GROUP BY skill ORDER BY jobs DESC LIMIT 20"
```

**Viewing the promotion queue:**
```bash
sqlite3 data/skills.db "SELECT term, taxonomy_category, jobs_count, status FROM taxonomy_candidates ORDER BY jobs_count DESC"
```

---

## Known issues & limitations

**LinkedIn SPA timing** — The library's `JobScraper` only waits for `domcontentloaded`, not for React to render. `job_scraper_direct.py` fixes this by waiting for `<h1>` to appear (up to 15s). If a page still returns no content, a screenshot and HTML snippet are saved to `data/debug/<job_id>.*` for inspection.

**No structured salary data** — LinkedIn hides salary behind a paywall / rarely includes it. The regex extraction catches it when it appears in the description text.

**Session expiry** — LinkedIn sessions typically last days to weeks. Re-run `setup_session.py` if you see "Session expired" errors.

**LinkedIn rate limiting** — Defaults: 3s between jobs, 5s between queries. Increase `DELAY_BETWEEN_JOBS` if you see rate-limit errors. The scraper retries once after a 60s pause on `RateLimitError`.

**LLM provider rate limiting (429)** — Groq's free tier caps at 100k tokens/day. `analyze.py` handles this automatically: parses the retry wait from the error, sleeps and retries if ≤ 2 minutes, otherwise falls back to `NINEROUTER_FALLBACK_MODEL`. To avoid interruptions on large batches, set `NINEROUTER_FALLBACK_MODEL = "9router-combo"` in `analyze.py` and configure a multi-provider combo in your 9router dashboard.

**CSS selectors may drift** — LinkedIn A/B tests its UI constantly. If `job_title` or `job_description` goes back to `null`, inspect `data/debug/` screenshots and update the selector lists in `job_scraper_direct.py`.
