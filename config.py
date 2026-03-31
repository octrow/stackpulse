"""Search configuration — edit this to control what gets scraped."""

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

# Jobs per search query (LinkedIn usually caps visible results around 25-100)
JOBS_PER_QUERY = 25

# Seconds to wait between scraping individual job pages (avoid rate limiting)
DELAY_BETWEEN_JOBS = 3

# Seconds to wait between different search queries
DELAY_BETWEEN_QUERIES = 5

# Where to store output
OUTPUT_DIR = "data"
SESSION_FILE = "session.json"
