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
from datetime import date, datetime
from pathlib import Path

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
        except Exception as e:
            log.warning(f"Could not read {json_file}: {e}")
    return seen_urls


def load_today_jobs(output_file: Path) -> list[dict]:
    """Load today's already-scraped jobs from disk, or return an empty list."""
    if not output_file.exists():
        return []
    try:
        return json.loads(output_file.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"Could not load existing output file: {e}")
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

async def scrape_all(limit_per_query: int, fresh: bool) -> None:
    """Run the full scrape across all configured queries."""
    try:
        from linkedin_scraper import BrowserManager, JobSearchScraper
        from linkedin_scraper import AuthenticationError
    except ImportError:
        log.error("linkedin-scraper not installed. Run: pip install -r requirements.txt && playwright install chromium")
        sys.exit(1)

    if not Path(SESSION_FILE).exists():
        log.error(f"{SESSION_FILE} not found. Run: py setup_session.py first.")
        sys.exit(1)

    today = date.today().isoformat()
    output_file = Path(OUTPUT_DIR) / f"jobs_{today}.json"

    # Always resume: collect every URL ever scraped (unless --fresh)
    if fresh:
        seen_urls: set[str] = set()
        all_jobs: list[dict] = []
        log.info("--fresh: ignoring all previous results")
    else:
        seen_urls = load_all_scraped_urls()
        all_jobs = load_today_jobs(output_file)
        log.info(
            f"Resume mode: {len(seen_urls)} URLs already seen across all files, "
            f"{len(all_jobs)} jobs in today's file"
        )

    total_queries = len(SEARCH_QUERIES)
    jobs_scraped_count = 0
    jobs_failed_count = 0
    start_time = datetime.now()

    log.info(f"Starting scrape — {total_queries} queries, limit={limit_per_query} each")

    async with BrowserManager(headless=True) as browser:
        await browser.load_session(SESSION_FILE)
        page = browser.page

        search_scraper = JobSearchScraper(page)

        for query_index, (keywords, location) in enumerate(SEARCH_QUERIES, 1):
            log.info(f"[{query_index}/{total_queries}] Searching '{keywords}' in '{location}'")

            try:
                job_urls: list[str] = await search_scraper.search(
                    keywords=keywords,
                    location=location,
                    limit=limit_per_query,
                )
            except AuthenticationError:
                log.error("Session expired — run setup_session.py again.")
                break
            except Exception as e:
                log.warning(f"Search failed: {e}")
                await asyncio.sleep(DELAY_BETWEEN_QUERIES)
                continue

            new_urls = [url for url in job_urls if url not in seen_urls]
            skipped_count = len(job_urls) - len(new_urls)
            log.info(f"  → {len(job_urls)} results, {len(new_urls)} new (skipping {skipped_count} already scraped)")

            for job_index, url in enumerate(new_urls, 1):
                seen_urls.add(url)
                log.info(f"  [{job_index}/{len(new_urls)}] Scraping {url}")

                try:
                    job_dict = await scrape_job(page, url)
                except AuthenticationError:
                    log.error("Session expired mid-scrape — saving and exiting.")
                    save_jobs(all_jobs, output_file)
                    return
                except Exception as e:
                    log.warning(f"  Failed: {e}")
                    jobs_failed_count += 1
                    await asyncio.sleep(DELAY_BETWEEN_JOBS)
                    continue

                job_dict["salary_extracted"] = extract_salary(job_dict.get("job_description"))
                job_dict["search_keywords"] = keywords
                job_dict["search_location"] = location
                job_dict["scraped_date"] = today

                description_length = len(job_dict.get("job_description") or "")
                salary_hint = (
                    f" | salary: {job_dict['salary_extracted']}"
                    if job_dict["salary_extracted"]
                    else ""
                )
                log.info(
                    f"  ✓ {job_dict.get('job_title') or '?'} @ {job_dict.get('company') or '?'} "
                    f"({job_dict.get('location') or '?'}) | desc={description_length}ch{salary_hint}"
                )

                all_jobs.append(job_dict)
                jobs_scraped_count += 1
                save_jobs(all_jobs, output_file)

                await asyncio.sleep(DELAY_BETWEEN_JOBS)

            await asyncio.sleep(DELAY_BETWEEN_QUERIES)

    elapsed = datetime.now() - start_time
    log.info(
        f"Done in {elapsed}. "
        f"New this session: {jobs_scraped_count}, failed: {jobs_failed_count}, "
        f"total in today's file: {len(all_jobs)}"
    )
    log.info(f"Output: {output_file.resolve()}")


def main() -> None:
    """Parse CLI arguments and launch the async scrape loop."""
    parser = argparse.ArgumentParser(description="StackPulse job scraper")
    parser.add_argument("--limit", type=int, default=JOBS_PER_QUERY, help="Max jobs per search query")
    parser.add_argument("--fresh", action="store_true", help="Re-scrape even already-seen URLs")
    args = parser.parse_args()

    asyncio.run(scrape_all(limit_per_query=args.limit, fresh=args.fresh))


if __name__ == "__main__":
    main()
