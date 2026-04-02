"""Search configuration — edit this to control what gets scraped."""

# ── Search queries ────────────────────────────────────────────────────────────

SEARCH_QUERIES = [
    # (keywords, location)
    ("Senior Backend Developer Python", "Berlin, Germany"),
    ("Senior Backend Developer Python", "Hamburg, Germany"),
    ("Senior Backend Developer Python", "Munich, Germany"),
    ("Senior Python Developer FastAPI", "Germany"),
    ("Senior Backend Developer Python", "Vienna, Austria"),
    ("Senior Backend Developer Python", "Amsterdam, Netherlands"),
    ("Senior Backend Developer Python", "Luxembourg"),
    ("Senior Backend Developer Python", "Barcelona, Spain"),
    ("Senior Backend Developer Python", "Madrid, Spain"),
    ("Senior Backend Developer Python", "London, United Kingdom"),
    ("Senior Backend Engineer Python FastAPI", "Remote"),
]

# Jobs per search query (LinkedIn usually caps visible results around 25–100)
JOBS_PER_QUERY = 25

# ── Scraping delays ───────────────────────────────────────────────────────────

# Seconds to wait between scraping individual job pages (avoid rate limiting)
DELAY_BETWEEN_JOBS = 3

# Seconds to wait between different search queries
DELAY_BETWEEN_QUERIES = 5

# Browser job search: LinkedIn paginates with `start=` (25 jobs per page in the UI).
JOB_SEARCH_PAGE_SIZE = 25
JOB_SEARCH_MAX_START = 1000
# Pause between paginated search page navigations (same query, next ``start``)
DELAY_BETWEEN_SEARCH_PAGES = 1.5

# ── Playwright timeouts & settle waits ───────────────────────────────────────

# Milliseconds to wait for the full page load (domcontentloaded)
PAGE_LOAD_TIMEOUT_MS = 60_000

# Milliseconds to wait for the job-title <h1> to appear (SPA render)
H1_WAIT_TIMEOUT_MS = 5_000

# Milliseconds to wait when clicking "Show more" / "See more" buttons
BUTTON_CLICK_TIMEOUT_MS = 3_000

# Seconds to sleep after each "Show more" button click
POST_CLICK_SETTLE_SECONDS = 0.5

# Seconds to sleep after all expand clicks, before extracting fields
POST_EXPAND_SETTLE_SECONDS = 1.0

# ── Debug output ──────────────────────────────────────────────────────────────

# Maximum characters saved in debug HTML snippets (data/debug/<id>.html)
DEBUG_HTML_SNIPPET_CHARS = 8_000

# ── Skill extraction ──────────────────────────────────────────────────────────

# Minimum character length for a scraped job description to be considered valid
DESCRIPTION_MIN_CHARS = 100

# ── Storage ───────────────────────────────────────────────────────────────────

# Where to store output JSON, logs, Excel exports, and the skills DB
OUTPUT_DIR = "data"

# SQLite database filename (placed inside OUTPUT_DIR)
DB_FILENAME = "skills.db"

# Rotating text log: same lines as the Rich "LLM & pipeline" panel (`analyze --llm` + verbose)
ANALYSIS_ACTIVITY_LOG_FILENAME = "analysis_activity.log"
ANALYSIS_ACTIVITY_LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MiB per file before rotation
ANALYSIS_ACTIVITY_LOG_BACKUP_COUNT = 5

# Saved Playwright browser session (cookies + localStorage)
SESSION_FILE = "session.json"

# ── Fast (HTTP / guest API) scraper ───────────────────────────────────────────
# Uses LinkedIn jobs-guest endpoints (no browser). Lower delays than Playwright;
# more aggressive IP-based rate limits — use sparingly or with proxies if needed.

FAST_DELAY_BETWEEN_JOBS = 1
FAST_DELAY_BETWEEN_QUERIES = 2
FAST_REQUEST_TIMEOUT = 15
FAST_SEARCH_PAGE_DELAY_MIN = 3
FAST_SEARCH_PAGE_DELAY_MAX = 7
FAST_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# When a scrape is interrupted (Ctrl+C), the next run continues from this query index
# (1-based, same order as SEARCH_QUERIES). Separate files for fast HTTP vs browser mode.
SCRAPER_RESUME_FAST_FILENAME = "scrape_resume_fast.json"
SCRAPER_RESUME_BROWSER_FILENAME = "scrape_resume_browser.json"

# ── LLM / 9router ─────────────────────────────────────────────────────────────

# Base URL of the local 9router OpenAI-compatible proxy
NINEROUTER_BASE_URL = "http://localhost:20128/v1"

# Primary model for LLM skill extraction. Use "9router-combo" when 9router should
# pick an available backend; use e.g. "groq/llama-3.3-70b-versatile" only if that
# provider is configured in 9router (otherwise every job 404s before fallback).
NINEROUTER_MODEL = "9router-combo"

# Fallback when primary fails (429 exhaustion, 404, parse errors, etc.).
# Example: "groq/llama-3.3-70b-versatile" if Groq is wired in 9router.
NINEROUTER_FALLBACK_MODEL = ""

# API key passed to the OpenAI-compatible client for 9router/local gateways.
NINEROUTER_API_KEY = "local"

# Maximum characters of job description text sent to the LLM
LLM_MAX_INPUT_CHARS = 8_000

# Maximum tokens the LLM may produce in a single extraction response
LLM_MAX_OUTPUT_TOKENS = 1_000

# Request OpenAI-compatible ``response_format: {type: json_object}`` for skill extraction.
# If the proxy returns an error about an unknown parameter, analyze.py retries once without it.
LLM_RESPONSE_FORMAT_JSON_OBJECT = True

# Maximum seconds to sleep-and-retry on a 429 rate-limit response.
# Waits longer than this are not worth blocking on; fall back to the fallback model.
LLM_RATE_LIMIT_MAX_WAIT_SECONDS = 30

# Buffer added to the parsed retry-after time so we don't hit the limit edge
RETRY_AFTER_BUFFER_SECONDS = 2

# Minimum job occurrences for a candidate term to be promoted into the skills
LLM_CANDIDATE_THRESHOLD = 2

# Rolling health check for `--llm` runs: compare stored LLM skill *rows* (sum of list lengths
# in the normalized JSON) to a rough regex baseline. Regex taxonomy often matches ~8–12 terms
# per posting; the skills-aware LLM stores far fewer rows (known matches + new discoveries only).
# If the last N jobs' combined row count falls below WARN_BELOW, emit a one-shot warning per
# "low" episode (see analyze.py).
LLM_LOW_SIGNAL_WINDOW_JOBS = 5
# Reference total rows for WINDOW_JOBS postings at typical regex richness (~9.6 / job).
LLM_LOW_SIGNAL_REFERENCE_SUM = 48
# Warn when rolling sum < this (50% of REFERENCE_SUM by default).
LLM_LOW_SIGNAL_WARN_BELOW_SUM = 24
