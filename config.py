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

# ── Playwright timeouts & settle waits ───────────────────────────────────────

# Milliseconds to wait for the full page load (domcontentloaded)
PAGE_LOAD_TIMEOUT_MS = 60_000

# Milliseconds to wait for the job-title <h1> to appear (SPA render)
H1_WAIT_TIMEOUT_MS = 15_000

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

# Saved Playwright browser session (cookies + localStorage)
SESSION_FILE = "session.json"

# ── LLM / 9router ─────────────────────────────────────────────────────────────

# Base URL of the local 9router OpenAI-compatible proxy
NINEROUTER_BASE_URL = "http://localhost:20128/v1"

# Primary model for LLM skill extraction
NINEROUTER_MODEL = "groq/llama-3.3-70b-versatile"

# Fallback model when the primary hits its daily quota.
# Set to a 9router combo name (e.g. "9router-combo") or another provider model.
# Leave empty ("") to skip on exhaustion.
# Recommended combo order: Cerebras/Llama-3.3-70B → Cerebras/GPT-OSS-120B →
#   Groq/Llama-4-Maverick → Together/Llama-3.3-70B-Turbo →
#   Fireworks/Llama-3.3-70B → Gemini-cli/Gemini-3-Flash → Kiro/Claude-Haiku-4.5
NINEROUTER_FALLBACK_MODEL = "9router-combo"

# Maximum characters of job description text sent to the LLM
LLM_MAX_INPUT_CHARS = 6_000

# Maximum tokens the LLM may produce in a single extraction response
LLM_MAX_OUTPUT_TOKENS = 800

# Maximum seconds to sleep-and-retry on a 429 rate-limit response.
# Waits longer than this are not worth blocking on; fall back to the fallback model.
LLM_RATE_LIMIT_MAX_WAIT_SECONDS = 30

# Buffer added to the parsed retry-after time so we don't hit the limit edge
RETRY_AFTER_BUFFER_SECONDS = 2

# Minimum job occurrences for a candidate term to be promoted into the taxonomy
LLM_CANDIDATE_THRESHOLD = 2
