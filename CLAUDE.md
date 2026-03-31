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
stackpulse --help
stackpulse setup-session
stackpulse scrape --limit 3
stackpulse scrape --fresh
stackpulse analyze --all --llm
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

# LLM → taxonomy promotion pipeline
py analyze.py --candidates           # inspect queue of LLM-discovered pending terms
py analyze.py --promote              # promote pending terms (≥2 jobs) into taxonomy
py analyze.py --promote 3            # same, threshold = 3 jobs
py analyze.py --all --promote        # promote first, then analyze with enriched taxonomy
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
- `setup-session` → `setup_session.main()`
- `scrape` → `scrape.scrape_all()`
- `analyze` → same workflow as `analyze.py main()` using existing helper functions
- `auto` → orchestrates venv/bootstrap/session/scrape/analyze sequence

Prefer reusing script-level functions and keep behavior parity with existing entrypoints.

## Architecture

The library (`linkedin_scraper`) provides two working pieces and one broken piece:

- **`BrowserManager`** — Playwright browser wrapper with session save/load. Used as an async context manager.
- **`JobSearchScraper`** — searches LinkedIn and returns a list of job URLs. Works correctly.
- **`JobScraper`** (library) — **broken**: only waits for `domcontentloaded`, so it reads an empty DOM before React renders. All fields return `null`. Do not use it.

`job_scraper_direct.py` is the drop-in replacement for `JobScraper`. It navigates to the job URL, waits for `<h1>` to appear, clicks description expand buttons, and extracts fields via ordered selector fallback lists. When `<h1>` never appears, it dumps a screenshot and HTML snippet to `OUTPUT_DIR/debug/<job_id>.*`.

`scrape.py` is now intentionally split into small orchestration helpers (`_search_query_urls`, `_scrape_query_urls`, `_log_run_summary`, etc.) to keep `scrape_all()` readable while preserving auto-resume and save-after-each-job semantics.

## Data flow

```
config.py (queries + timeouts + paths + LLM settings)
  → scrape.py               loops over queries, calls JobSearchScraper → list of URLs
  → job_scraper_direct.py   scrapes each URL → dict
  → data/jobs_YYYY-MM-DD.json   incremental save after each job
  → analyze.py              loads JSON(s)
      ├─ regex-matches taxonomy from data/skills.db → stdout report + .xlsx
      └─ (--llm) calls configured model via 9router → open skill extraction
                 on 429: sleeps parsed wait time (≤ LLM_RATE_LIMIT_MAX_WAIT_SECONDS) and retries once,
                         then falls back to NINEROUTER_FALLBACK_MODEL if configured
                 results stored in data/skills.db (llm_results table), cached per job URL
                 → promote_llm_to_candidates() auto-queues new terms in taxonomy_candidates
                 → py analyze.py --promote moves approved candidates into taxonomy
```

**`data/skills.db` tables:**
- `taxonomy(category, term)` — the skill list; seeded from `SKILLS_SEED` in `analyze.py` on first run; editable via SQL
- `llm_results(url_key, url, category, skill)` — LLM extraction results, one row per skill per job; keyed by MD5 of URL
- `llm_category_map(llm_category, taxonomy_category)` — maps LLM's 8 output categories to taxonomy categories; seeded once, editable via SQL
- `taxonomy_candidates(term, canonical, taxonomy_category, llm_category, jobs_count, status, added_date)` — promotion queue; `status` is `pending` / `approved` / `rejected`
- `term_aliases(taxonomy_id, alias, canonical, lang, alias_type)` — synonyms and multilingual variants for existing taxonomy terms; seeded with `python3`/`python 3`

## Key design decisions

**Auto-resume**: `scrape.py` always loads all URLs from every `data/jobs_*.json` on startup and skips them. There is no separate resume flag — use `--fresh` to override.

**Incremental save**: `save_jobs()` overwrites the output file after every single job. Safe to `Ctrl+C` at any time.

**Selector fallback pattern**: every extractor in `job_scraper_direct.py` tries a list of CSS selectors from most-specific to least-specific. LinkedIn A/B tests its UI, so specific class names drift. Generic fallbacks keep extraction working when class names change. Extraction errors are now logged with selector/button context while still failing soft to the next fallback.

**Salary**: not a structured LinkedIn field. `extract_salary()` in `scrape.py` regex-scans `job_description` text for currency patterns and stores the result in `salary_extracted`.

**Taxonomy term storage**: `taxonomy.term` stores the regex pattern, not the display string. Most terms are plain lowercase (e.g. `github actions`). Special characters must be pre-escaped (e.g. `c\+\+` for C++). `load_taxonomy()` unescapes patterns back to display strings via `re.sub(r"\\(.)", r"\1", term)`.

**LLM → taxonomy pipeline**: `--llm` extracts skills via LLM and caches results in `llm_results`. After each run, `promote_llm_to_candidates()` automatically queues terms seen in ≥ 2 jobs that are absent from taxonomy/alias coverage. `--promote` moves pending candidates into `taxonomy`, making them available in all future regex-based runs.

**Coverage gap vs queue status**: `--llm` reports two metrics: raw uncovered terms and actionable uncovered terms. Actionable terms require `jobs_count >= threshold`, exclusion from `SKIP_TERMS`, and absence from existing `taxonomy_candidates`. `--candidates` shows queue state only (`pending`/`approved`/`rejected`), so it will diverge from raw uncovered counts. Uncovered terms are printed as unescaped display text.

**LLM 429 / rate-limit handling**: `extract_skills_llm()` uses `_call_llm_with_retry()` and `_extract_skills_with_models()` to keep retry/fallback logic isolated. It parses suggested wait from the error message (supports `"try again in ..."` and `"reset after ..."`). If wait ≤ `LLM_RATE_LIMIT_MAX_WAIT_SECONDS` (default 30s), it sleeps and retries once. For longer waits (daily quota exhaustion), it falls back to `NINEROUTER_FALLBACK_MODEL`.

**`SKIP_TERMS`**: generic noise terms (`api`, `testing`, `automation`, etc.) that are blacklisted from entering `taxonomy_candidates`.

## Selector debugging

If job fields return `null` again, check `data/debug/`. The page title in the debug output tells you what happened:
- `"LinkedIn"` or `"Sign In | LinkedIn"` → session expired, re-run `setup_session.py`
- `"Senior ... | LinkedIn"` → page loaded but selectors need updating — inspect saved HTML and update selector lists in `job_scraper_direct.py`

## Extending search queries

Edit `SEARCH_QUERIES` in `config.py` — list of `(keywords, location)` tuples. LinkedIn location strings must match what LinkedIn search autocomplete accepts (e.g. `"Berlin, Germany"` not `"Berlin"`).

## Extending skill detection

Taxonomy is stored in `data/skills.db`, not hardcoded. Ways to add terms:

**Via the promotion pipeline (recommended):**
```bash
py analyze.py --llm    # extracts and queues candidates automatically
py analyze.py --promote
```

**Via SQL (immediate, no code change):**
```bash
sqlite3 data/skills.db "INSERT OR IGNORE INTO taxonomy(category,term) VALUES('Cloud','hetzner')"
```

**Via code (to persist across DB resets):** edit `SKILLS_SEED` in `analyze.py`. Terms are matched as whole words (`\b` boundary) against lowercased `job_title + job_description`. Escape special regex characters (e.g. `"c\\+\\+"` for C++, `"node\\.js"` for Node.js).

**Add a multilingual alias:**
```bash
sqlite3 data/skills.db \
  "INSERT INTO term_aliases(taxonomy_id,alias,canonical,lang,alias_type)
   SELECT id,'Deutsch','deutsch','de','translation' FROM taxonomy WHERE term='german'"
```

**Adjust LLM category → taxonomy category mapping:**
```bash
sqlite3 data/skills.db "UPDATE llm_category_map SET taxonomy_category='Testing' WHERE llm_category='concepts'"
```

To reset the DB and re-seed from `SKILLS_SEED`: `rm data/skills.db` then re-run `analyze.py`.
