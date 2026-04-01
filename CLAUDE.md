# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Optional: install CLI entrypoint
pip install -e .

# Typer + Rich CLI
stackpulse                              # interactive wizard (no-args default)
stackpulse --help
stackpulse setup-session
stackpulse scrape --limit 3
stackpulse scrape --fresh
stackpulse analyze --all --llm
stackpulse analyze --all --title-contains "Backend" --location-contains "Berlin"
stackpulse analyze --candidates
stackpulse auto --limit 3 --all

# One-time login (legacy script entrypoint still supported)
py setup_session.py

# Scrape jobs (legacy script entrypoints)
py scrape.py --limit 3      # test run
py scrape.py                # full run (all queries in config.py)
py scrape.py --fresh        # ignore all previous results

# Analyze collected data (legacy script entrypoints)
py analyze.py               # today's file
py analyze.py --all         # merge all data/jobs_*.json files
py analyze.py --llm         # + LLM extraction via 9router, results cached in skills.db
py analyze.py --all --llm

# LLM → skill promotion pipeline
py analyze.py --candidates           # inspect queue of LLM-discovered pending terms
py analyze.py --promote              # promote pending terms (≥2 jobs) into skills catalog
py analyze.py --promote 3            # same, threshold = 3 jobs
py analyze.py --all --promote        # promote first, then analyze with enriched skills
```

`stackpulse auto` defaults:

- creates `.venv` if missing
- installs dependencies only when missing
- installs Playwright Chromium only when missing
- skips session setup if `session.json` exists
- fails fast on first failed step

Legacy script commands remain valid and are used by the CLI internally.

## CLI architecture

`cli.py` is a thin Typer + Rich wrapper over existing module logic:

- no-args invocation → `_interactive_wizard()` (prompts for command + options, then calls the function directly)
- `setup-session` → `setup_session.main()`
- `scrape` → `scrape.scrape_all()`
- `analyze` → early-exit branches (candidates, promote-only, no-paths) in the command function; core pipeline delegated
  to `_run_analysis_pipeline()`; cohort filters (`--title-contains`, `--location-contains`) applied inside that helper
  after `load_jobs()`
- `auto` → orchestrates venv/bootstrap/session/scrape/analyze sequence

`analyze` DB connection is owned by the command function and closed in a `try/finally` — the pipeline helper never
closes it.

Analyze DB responsibilities are split by module:

- `analysis_db.py` — connection helpers, schema init/migrations, and `load_skills()`
- `analysis_candidates.py` — candidates queue read/write/promotion operations
- `analysis_llm_cache.py` — LLM cache key + DB cache read/write
- `analyze.py` — orchestration/reporting facade that reuses these helpers

Prefer reusing script-level functions and keep behavior parity with existing entrypoints.

## Architecture

The library (`linkedin_scraper`) provides two working pieces and one broken piece:

- **`BrowserManager`** — Playwright browser wrapper with session save/load. Used as an async context manager.
- **`JobSearchScraper`** — searches LinkedIn and returns a list of job URLs. Works correctly.
- **`JobScraper`** (library) — **broken**: only waits for `domcontentloaded`, so it reads an empty DOM before React
  renders. All fields return `null`. Do not use it.

`job_scraper_direct.py` is the drop-in replacement for `JobScraper`. It navigates to the job URL, waits for `<h1>` to
appear, clicks description expand buttons, and extracts fields via ordered selector fallback lists. When `<h1>` never
appears, it dumps a screenshot and HTML snippet to `OUTPUT_DIR/debug/<job_id>.*`.

`scrape.py` is now intentionally split into small orchestration helpers (`_search_query_urls`, `_scrape_query_urls`,
`_log_run_summary`, etc.) to keep `scrape_all()` readable while preserving auto-resume and save-after-each-job
semantics.

## Data flow

```
config.py (queries + timeouts + paths + LLM settings)
  → scrape.py               loops over queries, calls JobSearchScraper → list of URLs
  → job_scraper_direct.py   scrapes each URL → dict
  → data/jobs_YYYY-MM-DD.json   incremental save after each job
  → analyze.py              loads JSON(s)
      ├─ regex-matches skills from data/skills.db → stdout report + .xlsx
      └─ (--llm) calls configured model via 9router → open skill extraction
                 on 429: sleeps parsed wait time (≤ LLM_RATE_LIMIT_MAX_WAIT_SECONDS) and retries once,
                         then falls back to NINEROUTER_FALLBACK_MODEL if configured
                 results stored in data/skills.db (llm_results table), cached per job URL
                 → promote_llm_to_candidates() auto-queues new terms in skill_candidates
                 → py analyze.py --promote moves approved candidates into skills
```

**`data/skills.db` tables:**

- `categories(id, name)` — canonical category names; seeded from `SKILLS_SEED` keys on first run.
- `skills(id, category_id, term)` — the skill catalog; `category_id` FK → `categories`. Seeded from `SKILLS_SEED`
  in `analyze.py` on first run; editable via SQL.
  Terms are stored as plain lowercase text (e.g. `c++`, `node.js`); regex escaping is applied at load time only.
- `llm_results(url_key, url, category_id, skill, is_matched)` — LLM extraction results, one row per skill per job;
  keyed by MD5 of URL. `is_matched=1` for terms already in the skills catalog; `is_matched=0` for new discoveries.
  `category_id` FK → `categories`.
- `skill_candidates(id, term, category_id, llm_category_id, jobs_count, status, added_date, decided_date)` — promotion
  queue; both FK → `categories`. `status` is `pending` / `approved` / `rejected`.
- `skill_aliases(skill_id, alias, canonical, lang, alias_type)` — synonyms and multilingual variants for existing
  skills; seeded with `python3`/`python 3`

The LLM prompt instructs the model to use exact `categories.name` values for `new_terms[].category`. No mapping
layer needed — `_migrate_schema()` handles old-format rows on existing DBs.

## Key design decisions

**Auto-resume**: `scrape.py` always loads all URLs from every `data/jobs_*.json` on startup and skips them. There is no
separate resume flag — use `--fresh` to override.

**Incremental save**: `save_jobs()` overwrites the output file after every single job. Safe to `Ctrl+C` at any time.

**Selector fallback pattern**: every extractor in `job_scraper_direct.py` tries a list of CSS selectors from
most-specific to least-specific. LinkedIn A/B tests its UI, so specific class names drift. Generic fallbacks keep
extraction working when class names change. Extraction errors are now logged with selector/button context while still
failing soft to the next fallback.

**Salary**: not a structured LinkedIn field. `extract_salary()` in `scrape.py` regex-scans `job_description` text for
currency patterns and stores the result in `salary_extracted`.

**Skill term storage**: `skills.term` stores plain lowercase text (e.g. `c++`, `node.js`). `load_skills()` applies
`re.escape()` at load time to build regex patterns for word-boundary matching. No unescape helpers needed.

**Skills-aware LLM prompt**: `_build_llm_prompt(skills)` serializes all skill terms grouped by category into
the prompt so the LLM matches against known terms first. The LLM returns `{"matched": [...], "new_terms": [...]}`.
`_normalize_llm_result()` converts this to internal format with `"_matched"` key for known terms. When writing to DB,
`_llm_cache_set()` stores matched terms with `is_matched=1` and their actual skills category, new terms with
`is_matched=0`.

**LLM → skills pipeline**: `--llm` extracts skills via LLM and caches results in `llm_results`. After each run,
`promote_llm_to_candidates()` automatically queues terms seen in ≥ `LLM_CANDIDATE_THRESHOLD` jobs (default 2, set in
`config.py`) that are absent from skills/alias coverage. `is_matched=1` entries are excluded from candidate aggregation.
`--promote` moves pending candidates into `skills`, making them available in all future regex-based runs.

**Coverage gap vs queue status**: `--llm` reports two metrics: raw uncovered terms and actionable uncovered terms.
Because the LLM is skills-aware, uncovered terms are genuinely new discoveries — not synonyms or variants.
Actionable terms require `jobs_count >= threshold`, exclusion from `SKIP_TERMS`, and absence from existing
`skill_candidates`. `--candidates` shows queue state only (`pending`/`approved`/`rejected`), so it will diverge from
raw uncovered counts.

**LLM 429 / rate-limit handling**: `extract_skills_llm()` uses `_call_llm_with_retry()` and
`_extract_skills_with_models()` to keep retry/fallback logic isolated. It parses suggested wait from the error message (
supports `"try again in ..."` and `"reset after ..."`). If wait ≤ `LLM_RATE_LIMIT_MAX_WAIT_SECONDS` (default 30s), it
sleeps and retries once. For longer waits (daily quota exhaustion), it falls back to `NINEROUTER_FALLBACK_MODEL`.
Non-rate-limit errors (`APIError`, `APIConnectionError`, `JSONDecodeError`, `ValueError`) are caught specifically —
broad `except Exception` is not used.

**`SKIP_TERMS`**: generic noise terms (`api`, `testing`, `automation`, etc.) that are blacklisted from entering
`skill_candidates`.

**Public analyze API**: `resolve_input_paths(args, data_dir)` and `build_llm_client(base_url, model)` are public
functions (no leading underscore). `cli.py` calls them directly via `import analyze as analyzer`. `_VALID_DB_TABLES` is
an allowlist used by `_table_is_empty()` to guard against raw SQL table-name injection.

**Unified skills pipeline**: `analyze()` builds `skills_by_category` per job — regex hits merged with LLM-discovered
terms via `_build_comprehensive_by_category()` (case-insensitive dedup). Matched terms are routed to their skills
category via reverse lookup; new discoveries use their LLM-suggested category directly. All downstream consumers
(category breakdown, top-N, Excel export, per-location stats) use this unified view. `all_skills_flat` (regex-only) is
preserved for backward compatibility.

**Report additions**: `print_report` now calls `_print_quality_summary` (empty-description + zero-skill-job counts) and
`_print_skills_by_location` (top 3 skills per unique `search_location`, skipped when ≤1 location). Category breakdown
includes prevalence % per top term. The LLM section shows only skills coverage gaps (terms not yet in catalog),
not redundant LLM aggregates.

**Field coverage logging**: `_log_run_summary` logs counts of jobs missing `job_description`, `job_title`, and
`location` at end of each scrape run (visible in `data/scraper.log`).

## Selector debugging

If job fields return `null` again, check `data/debug/`. The page title in the debug output tells you what happened:

- `"LinkedIn"` or `"Sign In | LinkedIn"` → session expired, re-run `setup_session.py`
- `"Senior ... | LinkedIn"` → page loaded but selectors need updating — inspect saved HTML and update selector lists in
  `job_scraper_direct.py`

## Extending search queries

Edit `SEARCH_QUERIES` in `config.py` — list of `(keywords, location)` tuples. LinkedIn location strings must match what
LinkedIn search autocomplete accepts (e.g. `"Berlin, Germany"` not `"Berlin"`).

## Extending skill detection

Skills catalog is stored in `data/skills.db`, not hardcoded. Ways to add terms:

**Via the promotion pipeline (recommended):**

```bash
py analyze.py --llm    # extracts and queues candidates automatically
py analyze.py --promote
```

**Via SQL (immediate, no code change):**

```bash
sqlite3 data/skills.db "INSERT OR IGNORE INTO skills(category_id,term) SELECT id,'hetzner' FROM categories WHERE name='Cloud'"
```

**Via code (to persist across DB resets):** edit `SKILLS_SEED` in `analyze.py`. Terms are stored as plain lowercase
text and matched as whole words (`\b` boundary) against lowercased `job_title + job_description`. No escaping needed
in the seed — `normalize_term()` handles it; `re.escape()` is applied at load time.

**Add a multilingual alias:**

```bash
sqlite3 data/skills.db \
  "INSERT INTO skill_aliases(skill_id,alias,canonical,lang,alias_type)
   SELECT id,'Deutsch','deutsch','de','translation' FROM skills WHERE term='german'"
```

To reset the DB and re-seed from `SKILLS_SEED`: `rm data/skills.db` then re-run `analyze.py`.
