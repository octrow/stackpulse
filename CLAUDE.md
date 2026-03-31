# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# One-time login (saves session.json)
py setup_session.py

# Scrape jobs
py scrape.py --limit 3      # test run
py scrape.py                # full run (all queries in config.py)
py scrape.py --fresh        # ignore all previous results

# Analyze collected data
py analyze.py               # today's file
py analyze.py --all         # merge all data/jobs_*.json files
py analyze.py --llm         # + LLM extraction via 9router (localhost:20128), results cached in skills.db
py analyze.py --all --llm

# LLM → taxonomy promotion pipeline
py analyze.py --candidates           # inspect queue of LLM-discovered pending terms
py analyze.py --promote              # promote pending terms (≥2 jobs) into taxonomy
py analyze.py --promote 3            # same, threshold = 3 jobs
py analyze.py --all --promote        # promote first, then analyze with enriched taxonomy
```

## Architecture

The library (`linkedin_scraper`) provides two working pieces and one broken piece:

- **`BrowserManager`** — Playwright browser wrapper with session save/load. Used as an async context manager.
- **`JobSearchScraper`** — searches LinkedIn and returns a list of job URLs. Works correctly.
- **`JobScraper`** (library) — **broken**: only waits for `domcontentloaded`, so it reads an empty DOM before React renders. All fields return `null`. Do not use it.

`job_scraper_direct.py` is the drop-in replacement for `JobScraper`. It navigates to the job URL, waits for `<h1>` to appear (up to 15s), clicks "Show more" buttons to expand the description, then extracts fields using ordered lists of CSS selector fallbacks. When `<h1>` never appears (auth wall, redirect, etc.) it dumps a screenshot and HTML snippet to `data/debug/<job_id>.*`.

## Data flow

```
config.py (queries)
  → scrape.py               loops over queries, calls JobSearchScraper → list of URLs
  → job_scraper_direct.py   scrapes each URL → dict
  → data/jobs_YYYY-MM-DD.json   incremental save after each job
  → analyze.py              loads JSON(s)
      ├─ regex-matches taxonomy from data/skills.db → stdout report + .xlsx
      └─ (--llm) calls groq/llama-3.3-70b-versatile via 9router → open skill extraction
                 on 429: sleeps parsed wait time (≤ MAX_429_WAIT_S=120s) and retries once,
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

**Selector fallback pattern**: every extractor in `job_scraper_direct.py` tries a list of CSS selectors from most-specific to least-specific. LinkedIn A/B tests its UI, so specific class names drift. The generic fallbacks (`h1`, `span`, `article`) keep extraction working when class names change.

**Salary**: not a structured LinkedIn field. `extract_salary()` in `scrape.py` regex-scans `job_description` text for currency patterns and stores the result in `salary_extracted`.

**Taxonomy term storage**: `taxonomy.term` stores the regex pattern, not the display string. Most terms are plain lowercase (e.g. `github actions`). Special characters must be pre-escaped (e.g. `c\+\+` for C++). `load_taxonomy()` unescapes patterns back to display strings via `re.sub(r"\\(.)", r"\1", term)`.

**LLM → taxonomy pipeline**: `--llm` extracts skills via LLM and caches results in `llm_results`. After each run, `promote_llm_to_candidates()` automatically queues terms seen in ≥ 2 jobs that are absent from the taxonomy. `--promote` moves pending candidates into `taxonomy`, making them available in all future regex-based runs.

**LLM 429 / rate-limit handling**: `extract_skills_llm()` catches `RateLimitError` and parses the suggested wait time from the error message (handles both `"try again in 18m0s"` and `"reset after 4s"` formats). If wait ≤ `MAX_429_WAIT_S` (120 s), it sleeps and retries once. If the wait is longer (daily quota exhausted), it falls through to `NINEROUTER_FALLBACK_MODEL`. Set that constant to a 9router combo name (e.g. `"9router-combo"`) to enable automatic provider failover. Recommended combo order: Cerebras/Llama-3.3-70B → Cerebras/GPT-OSS-120B → Groq/Llama-4-Maverick → Together/Llama-3.3-70B-Turbo → Fireworks/Llama-3.3-70B → Gemini-cli/Gemini-3-Flash → Kiro/Claude-Haiku-4.5.

**`SKIP_TERMS`**: generic noise terms (`api`, `testing`, `automation`, etc.) that are blacklisted from ever entering `taxonomy_candidates`, defined as a set constant at the top of `analyze.py`.

## Selector debugging

If job fields return `null` again, check `data/debug/`. The page `title` in the debug print tells you what happened:
- `"LinkedIn"` or `"Sign In | LinkedIn"` → session expired, re-run `setup_session.py`
- `"Senior ... | LinkedIn"` → page loaded but selectors need updating — inspect the saved HTML and update the selector lists in `job_scraper_direct.py`

## Extending search queries

Edit `SEARCH_QUERIES` in `config.py` — list of `(keywords, location)` tuples. LinkedIn location strings must match what LinkedIn's search autocomplete accepts (e.g. `"Berlin, Germany"` not `"Berlin"`).

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
