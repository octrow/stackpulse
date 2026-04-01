"""
Main scraper. Searches LinkedIn for jobs using config.py queries,
scrapes full details for each posting, and saves results to data/.

Already-scraped URLs (across ALL previous runs) are always skipped automatically.

Usage:
    py scrape.py                  # full run
    py scrape.py --limit 5        # max 5 jobs per query (quick test)
    py scrape.py --fresh          # ignore previous results, re-scrape everything
"""

import asyncio
import argparse
import json
import logging
import re
import sys
from json import JSONDecodeError
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from playwright.async_api import Error as PlaywrightError
except ImportError:  # pragma: no cover - only when playwright is missing
    PlaywrightError = RuntimeError

from config import (
    SEARCH_QUERIES,
    JOBS_PER_QUERY,
    DELAY_BETWEEN_JOBS,
    DELAY_BETWEEN_QUERIES,
    OUTPUT_DIR,
    SESSION_FILE,
)
from job_scraper_direct import scrape_job


# ── Logging setup ─────────────────────────────────────────────────────────────


def setup_logging() -> logging.Logger:
    """Configure file + stdout logging and silence noisy third-party loggers."""
    log_dir = Path(OUTPUT_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "scraper.log"

    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.DEBUG,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Silence noisy third-party loggers
    for noisy_logger_name in ("playwright", "asyncio", "urllib3", "httpx"):
        logging.getLogger(noisy_logger_name).setLevel(logging.WARNING)

    return logging.getLogger("scraper")


# Module-level logger; setup runs on import intentionally (script entry point).
log = setup_logging()


# ── Helpers ───────────────────────────────────────────────────────────────────


def load_all_scraped_urls() -> set[str]:
    """Return every URL collected across all previous JSON files."""
    data_dir = Path(OUTPUT_DIR)
    seen_urls: set[str] = set()
    for json_file in sorted(data_dir.glob("jobs_*.json")):
        try:
            jobs = json.loads(json_file.read_text(encoding="utf-8"))
            for job in jobs:
                url = job.get("linkedin_url")
                if url:
                    seen_urls.add(url)
        except (OSError, JSONDecodeError, TypeError) as error:
            log.warning(
                "Could not read %s (%s): %s",
                json_file,
                type(error).__name__,
                error,
            )
    return seen_urls


def load_today_jobs(output_file: Path) -> list[dict]:
    """Load today's already-scraped jobs from disk, or return an empty list."""
    if not output_file.exists():
        return []
    try:
        return json.loads(output_file.read_text(encoding="utf-8"))
    except (OSError, JSONDecodeError, TypeError) as error:
        log.warning(
            "Could not load existing output file %s (%s): %s",
            output_file,
            type(error).__name__,
            error,
        )
        return []


def save_jobs(jobs: list[dict], output_file: Path) -> None:
    """Write jobs list to disk as pretty-printed JSON (overwrites on each call)."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(
        json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.debug(f"Saved {len(jobs)} jobs → {output_file}")


def extract_salary(text: str | None) -> str | None:
    """Regex-scan job description text for salary/compensation patterns.

    Returns the first match as a raw string, or None if nothing found.
    LinkedIn does not expose salary as a structured field.
    """
    if not text:
        return None
    patterns = [
        r"[\$€£]\s?\d[\d,\.]+\s?[-–]\s?[\$€£]?\s?\d[\d,\.]+\s*(?:k|K)?(?:\s*(?:per|/)\s*(?:year|yr|month|mo|annum))?",
        r"\d[\d,\.]+\s?[-–]\s?\d[\d,\.]+\s*(?:k|K)?\s*(?:EUR|GBP|USD|€|£|\$)",
        r"(?:salary|compensation|pay)[^\n]{0,60}[\$€£]\s?\d[\d,\.]+",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0).strip()
    return None


# ── Main scrape loop ──────────────────────────────────────────────────────────


def _load_scraper_dependencies() -> tuple[Any, Any, type[Exception]]:
    """Import runtime scraper dependencies or exit with a clear instruction."""
    try:
        from linkedin_scraper import BrowserManager, JobSearchScraper
        from linkedin_scraper import AuthenticationError
    except ImportError:
        log.error(
            "linkedin-scraper not installed. Run: pip install -r requirements.txt && playwright install chromium"
        )
        sys.exit(1)
    return BrowserManager, JobSearchScraper, AuthenticationError


def _ensure_session_file() -> None:
    """Exit early when no saved LinkedIn session is available."""
    if not Path(SESSION_FILE).exists():
        log.error("%s not found. Run: py setup_session.py first.", SESSION_FILE)
        sys.exit(1)


def _initialise_scrape_state(
    output_file: Path, fresh: bool
) -> tuple[set[str], list[dict]]:
    """Return seen URLs and current output rows for fresh/resume mode."""
    if fresh:
        log.info("--fresh: ignoring all previous results")
        return set(), []

    seen_urls = load_all_scraped_urls()
    all_jobs = load_today_jobs(output_file)
    log.info(
        "Resume mode: %s URLs already seen across all files, %s jobs in today's file",
        len(seen_urls),
        len(all_jobs),
    )
    return seen_urls, all_jobs


def _enrich_scraped_job(
    job: dict, keywords: str, location: str, scraped_date: str
) -> None:
    """Attach derived and provenance fields to a scraped job record."""
    job["salary_extracted"] = extract_salary(job.get("job_description"))
    job["search_keywords"] = keywords
    job["search_location"] = location
    job["scraped_date"] = scraped_date


def _log_scraped_job(job: dict) -> None:
    """Log a compact successful-scrape summary line."""
    description_length = len(job.get("job_description") or "")
    salary_hint = (
        f" | salary: {job['salary_extracted']}" if job["salary_extracted"] else ""
    )
    log.info(
        "  ✓ %s @ %s (%s) | desc=%sch%s",
        job.get("job_title") or "?",
        job.get("company") or "?",
        job.get("location") or "?",
        description_length,
        salary_hint,
    )


async def _search_query_urls(
    search_scraper: Any,
    keywords: str,
    location: str,
    limit_per_query: int,
    authentication_error_cls: type[Exception],
) -> tuple[list[str], bool]:
    """Search one query and return URLs + stop signal for auth expiry."""
    try:
        job_urls: list[str] = await search_scraper.search(
            keywords=keywords,
            location=location,
            limit=limit_per_query,
        )
        return job_urls, False
    except authentication_error_cls:
        log.error("Session expired — run setup_session.py again.")
        return [], True
    except (PlaywrightError, TimeoutError, OSError, ValueError) as error:
        log.warning(
            "Search failed for '%s' in '%s' (%s): %s",
            keywords,
            location,
            type(error).__name__,
            error,
        )
        await asyncio.sleep(DELAY_BETWEEN_QUERIES)
        return [], False


async def _scrape_query_urls(
    page: Any,
    new_urls: list[str],
    seen_urls: set[str],
    all_jobs: list[dict],
    output_file: Path,
    keywords: str,
    location: str,
    scraped_date: str,
    authentication_error_cls: type[Exception],
) -> tuple[int, int, bool]:
    """Scrape all URLs for one query and return counts + early-stop signal."""
    jobs_scraped_count = 0
    jobs_failed_count = 0

    for job_index, url in enumerate(new_urls, 1):
        seen_urls.add(url)
        log.info("  [%s/%s] Scraping %s", job_index, len(new_urls), url)

        try:
            job_dict = await scrape_job(page, url)
        except authentication_error_cls:
            log.error("Session expired mid-scrape — saving and exiting.")
            save_jobs(all_jobs, output_file)
            return jobs_scraped_count, jobs_failed_count, True
        except (PlaywrightError, TimeoutError, OSError, ValueError) as error:
            log.warning(
                "  Failed scraping %s (%s): %s", url, type(error).__name__, error
            )
            jobs_failed_count += 1
            await asyncio.sleep(DELAY_BETWEEN_JOBS)
            continue

        _enrich_scraped_job(job_dict, keywords, location, scraped_date)
        _log_scraped_job(job_dict)

        all_jobs.append(job_dict)
        jobs_scraped_count += 1
        save_jobs(all_jobs, output_file)
        await asyncio.sleep(DELAY_BETWEEN_JOBS)

    return jobs_scraped_count, jobs_failed_count, False


def _log_run_summary(
    start_time: datetime,
    jobs_scraped_count: int,
    jobs_failed_count: int,
    all_jobs: list[dict],
    output_file: Path,
) -> None:
    """Log end-of-run summary metrics and output path."""
    elapsed = datetime.now() - start_time
    log.info(
        "Done in %s. New this session: %s, failed: %s, total in today's file: %s",
        elapsed,
        jobs_scraped_count,
        jobs_failed_count,
        len(all_jobs),
    )
    if all_jobs:
        missing_desc = sum(1 for j in all_jobs if not j.get("job_description"))
        missing_title = sum(1 for j in all_jobs if not j.get("job_title"))
        missing_loc = sum(1 for j in all_jobs if not j.get("location"))
        log.info(
            "Field coverage — missing description: %s, missing title: %s, missing location: %s",
            missing_desc,
            missing_title,
            missing_loc,
        )
    log.info("Output: %s", output_file.resolve())


async def scrape_all(limit_per_query: int, fresh: bool) -> None:
    """Run the full scrape across all configured queries."""
    browser_manager_cls, job_search_scraper_cls, authentication_error_cls = (
        _load_scraper_dependencies()
    )
    _ensure_session_file()

    today = date.today().isoformat()
    output_file = Path(OUTPUT_DIR) / f"jobs_{today}.json"
    seen_urls, all_jobs = _initialise_scrape_state(output_file, fresh)

    total_queries = len(SEARCH_QUERIES)
    jobs_scraped_count = 0
    jobs_failed_count = 0
    start_time = datetime.now()

    log.info(
        "Starting scrape — %s queries, limit=%s each", total_queries, limit_per_query
    )

    _interrupted = False

    async with browser_manager_cls(headless=True) as browser:
        await browser.load_session(SESSION_FILE)
        search_scraper = job_search_scraper_cls(browser.page)

        try:
            for query_index, (keywords, location) in enumerate(SEARCH_QUERIES, 1):
                log.info(
                    "[%s/%s] Searching '%s' in '%s'",
                    query_index,
                    total_queries,
                    keywords,
                    location,
                )

                job_urls, should_stop = await _search_query_urls(
                    search_scraper,
                    keywords,
                    location,
                    limit_per_query,
                    authentication_error_cls,
                )
                if should_stop:
                    break
                if not job_urls:
                    continue

                new_urls = [url for url in job_urls if url not in seen_urls]
                skipped_count = len(job_urls) - len(new_urls)
                log.info(
                    "  → %s results, %s new (skipping %s already scraped)",
                    len(job_urls),
                    len(new_urls),
                    skipped_count,
                )

                (
                    query_scraped_count,
                    query_failed_count,
                    should_stop,
                ) = await _scrape_query_urls(
                    browser.page,
                    new_urls,
                    seen_urls,
                    all_jobs,
                    output_file,
                    keywords,
                    location,
                    today,
                    authentication_error_cls,
                )
                jobs_scraped_count += query_scraped_count
                jobs_failed_count += query_failed_count
                if should_stop:
                    return

                await asyncio.sleep(DELAY_BETWEEN_QUERIES)
        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info(
                "Scrape interrupted — saving %d jobs collected so far.", len(all_jobs)
            )
            _interrupted = True
            # Do not re-raise: lets async with __aexit__ close the browser cleanly

    _log_run_summary(
        start_time, jobs_scraped_count, jobs_failed_count, all_jobs, output_file
    )

    if _interrupted:
        raise KeyboardInterrupt


def main() -> None:
    """Parse CLI arguments and launch the async scrape loop."""
    parser = argparse.ArgumentParser(description="StackPulse job scraper")
    parser.add_argument(
        "--limit", type=int, default=JOBS_PER_QUERY, help="Max jobs per search query"
    )
    parser.add_argument(
        "--fresh", action="store_true", help="Re-scrape even already-seen URLs"
    )
    args = parser.parse_args()

    try:
        asyncio.run(scrape_all(limit_per_query=args.limit, fresh=args.fresh))
    except KeyboardInterrupt:
        log.info("Interrupted by user. Exiting cleanly.")
        sys.exit(130)


if __name__ == "__main__":
    main()
