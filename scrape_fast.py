"""
Fast LinkedIn job scraper using HTTP requests to guest job endpoints.

No Playwright session required. Based on the same approach as JobSpy
(/jobs-guest/jobs/api/seeMoreJobPostings/search + /jobs/view/{id}).

Usage:
    py scrape_fast.py --limit 5
    py scrape_fast.py --fresh
If you interrupt (Ctrl+C), the next run continues from the same search query (see data/scrape_resume_fast.json).
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import random
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from analysis_db import canonical_linkedin_job_key, upsert_scraped_job_key
from config import (
    FAST_DELAY_BETWEEN_JOBS,
    FAST_DELAY_BETWEEN_QUERIES,
    FAST_REQUEST_TIMEOUT,
    FAST_SEARCH_PAGE_DELAY_MAX,
    FAST_SEARCH_PAGE_DELAY_MIN,
    FAST_USER_AGENT,
    JOBS_PER_QUERY,
    OUTPUT_DIR,
    SCRAPER_RESUME_FAST_FILENAME,
    SEARCH_QUERIES,
)
from scrape import (
    clear_scrape_resume,
    enrich_scraped_job,
    initialise_scrape_state,
    load_scrape_resume_query_index,
    log_run_summary,
    open_scrape_db,
    persist_scrape_resume_pointer,
    save_jobs,
    setup_logging,
)

log = setup_logging()

LINKEDIN_BASE = "https://www.linkedin.com"

_DEFAULT_HEADERS = {
    "authority": "www.linkedin.com",
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
        "image/apng,*/*;q=0.8"
    ),
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "upgrade-insecure-requests": "1",
    "user-agent": FAST_USER_AGENT,
}


def _normalise_company_url(href: str | None) -> str | None:
    if not href:
        return None
    href = href.split("?")[0]
    if href and not href.startswith("http"):
        href = LINKEDIN_BASE + href
    return href or None


def _create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_DEFAULT_HEADERS)
    return session


def _run_sync(session: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
    fn = getattr(session, method)
    return fn(url, **kwargs)


async def _request(
    session: requests.Session,
    method: str,
    url: str,
    **kwargs: Any,
) -> requests.Response:
    loop = asyncio.get_running_loop()
    timeout = kwargs.pop("timeout", FAST_REQUEST_TIMEOUT)
    return await loop.run_in_executor(
        None,
        functools.partial(_run_sync, session, method, url, timeout=timeout, **kwargs),
    )


def _parse_search_card(job_card: Tag, job_id: str) -> dict[str, Any]:
    """Extract listing fields from a base-search-card (JobSpy-aligned)."""
    href_tag = job_card.find("a", class_="base-card__full-link")
    linkedin_url = f"{LINKEDIN_BASE}/jobs/view/{job_id}/"
    if href_tag and href_tag.get("href"):
        linkedin_url = href_tag["href"].split("?")[0]

    title_tag = job_card.find("span", class_="sr-only")
    job_title = title_tag.get_text(strip=True) if title_tag else None

    company_tag = job_card.find("h4", class_="base-search-card__subtitle")
    company_a = company_tag.find("a") if company_tag else None
    company = company_a.get_text(strip=True) if company_a else None
    company_url = None
    if company_a and company_a.has_attr("href"):
        company_url = _normalise_company_url(company_a["href"])

    metadata = job_card.find("div", class_="base-search-card__metadata")
    location_str = None
    posted_date = None
    if metadata:
        loc_tag = metadata.find("span", class_="job-search-card__location")
        if loc_tag:
            location_str = loc_tag.get_text(strip=True)
        dt_tag = metadata.find("time", class_="job-search-card__listdate")
        if not dt_tag:
            dt_tag = metadata.find("time", class_="job-search-card__listdate--new")
        if dt_tag:
            if dt_tag.get("datetime"):
                try:
                    posted_date = datetime.strptime(
                        dt_tag["datetime"], "%Y-%m-%d"
                    ).date().isoformat()
                except ValueError:
                    posted_date = dt_tag.get_text(strip=True)
            else:
                posted_date = dt_tag.get_text(strip=True)

    return {
        "linkedin_url": linkedin_url,
        "job_id": job_id,
        "job_title": job_title,
        "company": company,
        "company_linkedin_url": company_url,
        "location": location_str,
        "posted_date": posted_date,
    }


def _parse_job_detail_html(html: str) -> dict[str, Any | None]:
    """Parse guest-visible job page for description and fallbacks."""
    soup = BeautifulSoup(html, "lxml")
    out: dict[str, Any | None] = {
        "job_description": None,
        "job_title": None,
        "company": None,
        "company_linkedin_url": None,
        "location": None,
    }

    desc_div = soup.find(
        "div",
        class_=lambda x: isinstance(x, str) and "show-more-less-html__markup" in x,
    )
    if desc_div:
        out["job_description"] = desc_div.get_text(separator="\n", strip=True) or None

    for h1_sel in ("h1.t-24", "h1.jobs-unified-top-card__job-title", "h1"):
        h1 = soup.select_one(h1_sel)
        if h1 and h1.get_text(strip=True):
            out["job_title"] = h1.get_text(strip=True)
            break

    for a in soup.select("a.app-aware-link[href*='/company/'], a[href*='/company/']"):
        t = a.get_text(strip=True)
        if t and len(t) > 1 and "logo" not in t.lower():
            href = a.get("href") or ""
            out["company"] = t
            out["company_linkedin_url"] = _normalise_company_url(href)
            break

    return out


async def _fetch_job_detail(
    session: requests.Session, job_id: str
) -> dict[str, Any | None]:
    url = f"{LINKEDIN_BASE}/jobs/view/{job_id}"
    try:
        resp = await _request(session, "get", url)
    except requests.RequestException as exc:
        log.warning("Detail request failed for %s: %s", job_id, exc)
        return None

    if resp.status_code == 429:
        log.error("429 from LinkedIn on job detail — slow down or use proxies.")
        return None
    if resp.status_code not in range(200, 400):
        log.warning("Detail %s status %s", job_id, resp.status_code)
        return None
    if "linkedin.com/signup" in (resp.url or ""):
        log.debug("Job %s redirected to signup — no guest description", job_id)
        return {}

    return _parse_job_detail_html(resp.text)


def _merge_job_record(
    listing: dict[str, Any], detail: dict[str, Any | None] | None
) -> dict[str, Any]:
    """Build final job dict matching job_scraper_direct / analyze expectations."""
    job: dict[str, Any] = {
        "linkedin_url": listing["linkedin_url"],
        "job_title": listing.get("job_title"),
        "company": listing.get("company"),
        "company_linkedin_url": listing.get("company_linkedin_url"),
        "location": listing.get("location"),
        "posted_date": listing.get("posted_date"),
        "applicant_count": None,
        "job_description": None,
        "benefits": None,
    }
    if detail:
        if detail.get("job_description"):
            job["job_description"] = detail["job_description"]
        if detail.get("job_title"):
            job["job_title"] = detail["job_title"]
        if detail.get("company"):
            job["company"] = detail["company"]
        if detail.get("company_linkedin_url"):
            job["company_linkedin_url"] = detail["company_linkedin_url"]
        if detail.get("location"):
            job["location"] = detail["location"]
    return job


async def _search_query_fast(
    session: requests.Session,
    keywords: str,
    location: str,
    results_wanted: int,
) -> list[dict[str, Any]]:
    """Return listing dicts (one per job id) from guest search API HTML."""
    listings: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    start = 0

    while len(listings) < results_wanted and start < 1000:
        params = {
            "keywords": keywords,
            "location": location,
            "pageNum": 0,
            "start": start,
        }
        url = f"{LINKEDIN_BASE}/jobs-guest/jobs/api/seeMoreJobPostings/search"
        try:
            resp = await _request(session, "get", url, params=params)
        except requests.RequestException as exc:
            log.warning("Search request failed (%s): %s", type(exc).__name__, exc)
            break

        if resp.status_code == 429:
            log.error("429 on LinkedIn search — blocked; try again later or use browser mode.")
            break
        if resp.status_code not in range(200, 400):
            log.warning("Search returned status %s", resp.status_code)
            break

        soup = BeautifulSoup(resp.text, "lxml")
        job_cards = soup.find_all("div", class_="base-search-card")
        if not job_cards:
            log.debug("No job cards at start=%s — end of results", start)
            break

        for job_card in job_cards:
            if len(listings) >= results_wanted:
                break
            href_tag = job_card.find("a", class_="base-card__full-link")
            if not href_tag or "href" not in href_tag.attrs:
                continue
            href = href_tag["href"].split("?")[0]
            job_id = href.rstrip("/").split("-")[-1]
            if not job_id.isdigit():
                m = re.search(r"/jobs/view/(\d+)", href)
                job_id = m.group(1) if m else ""
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)
            listings.append(_parse_search_card(job_card, job_id))

        if len(listings) >= results_wanted:
            break

        await asyncio.sleep(
            random.uniform(FAST_SEARCH_PAGE_DELAY_MIN, FAST_SEARCH_PAGE_DELAY_MAX)
        )
        start += len(job_cards)

    return listings[:results_wanted]


async def scrape_all_fast(limit_per_query: int, fresh: bool) -> bool:
    """HTTP-based scrape: same outputs and dedupe as scrape.scrape_all.

    Returns True if the user interrupted (Ctrl+C), False on normal completion.
    """
    conn = open_scrape_db()
    try:
        today = date.today().isoformat()
        data_dir = Path(OUTPUT_DIR)
        output_file = data_dir / f"jobs_{today}.json"
        seen_keys, all_jobs = initialise_scrape_state(output_file, fresh, conn)

        if fresh:
            clear_scrape_resume(data_dir, browser=False)

        start_q = 1 if fresh else load_scrape_resume_query_index(data_dir, browser=False)
        queries_slice = SEARCH_QUERIES[start_q - 1 :]
        total_q = len(SEARCH_QUERIES)

        jobs_scraped_count = 0
        jobs_failed_count = 0
        start_time = datetime.now()
        session = _create_session()

        log.info(
            "Starting FAST (HTTP) scrape — %s queries, limit=%s each",
            total_q,
            limit_per_query,
        )
        if start_q > 1 and not fresh:
            log.info(
                "Resume: continuing from query %s/%s (delete %s to start from query 1)",
                start_q,
                total_q,
                SCRAPER_RESUME_FAST_FILENAME,
            )

        _interrupted = False
        resume_next = start_q

        try:
            for query_index, (keywords, location) in enumerate(queries_slice, start=start_q):
                resume_next = query_index
                log.info(
                    "[%s/%s] Searching '%s' in '%s'",
                    query_index,
                    total_q,
                    keywords,
                    location,
                )

                listings = await _search_query_fast(
                    session, keywords, location, limit_per_query
                )
                if not listings:
                    await asyncio.sleep(FAST_DELAY_BETWEEN_QUERIES)
                    resume_next = query_index + 1
                    persist_scrape_resume_pointer(data_dir, resume_next, browser=False)
                    continue

                new_listings = []
                for listing in listings:
                    url = listing["linkedin_url"]
                    key = canonical_linkedin_job_key(url)
                    if key and key not in seen_keys:
                        new_listings.append(listing)

                skipped = len(listings) - len(new_listings)
                log.info(
                    "  → %s results, %s new (skipping %s already scraped)",
                    len(listings),
                    len(new_listings),
                    skipped,
                )

                for job_index, listing in enumerate(new_listings, 1):
                    url = listing["linkedin_url"]
                    job_id = listing["job_id"]
                    log.info("  [%s/%s] Fetching %s", job_index, len(new_listings), url)

                    detail = await _fetch_job_detail(session, job_id)
                    if detail is None:
                        jobs_failed_count += 1
                        await asyncio.sleep(FAST_DELAY_BETWEEN_JOBS)
                        continue

                    job_dict = _merge_job_record(listing, detail)
                    enrich_scraped_job(job_dict, keywords, location, today)
                    salary_hint = (
                        f" | salary: {job_dict['salary_extracted']}"
                        if job_dict.get("salary_extracted")
                        else ""
                    )
                    log.info(
                        "  ✓ %s @ %s (%s) | desc=%sch%s",
                        job_dict.get("job_title") or "?",
                        job_dict.get("company") or "?",
                        job_dict.get("location") or "?",
                        len(job_dict.get("job_description") or ""),
                        salary_hint,
                    )

                    all_jobs.append(job_dict)
                    jobs_scraped_count += 1
                    save_jobs(all_jobs, output_file)

                    key = canonical_linkedin_job_key(url)
                    if key:
                        upsert_scraped_job_key(
                            conn,
                            key,
                            url,
                            datetime.now().isoformat(timespec="seconds"),
                        )
                        seen_keys.add(key)

                    await asyncio.sleep(FAST_DELAY_BETWEEN_JOBS)

                resume_next = query_index + 1
                persist_scrape_resume_pointer(data_dir, resume_next, browser=False)
                await asyncio.sleep(FAST_DELAY_BETWEEN_QUERIES)

        except (KeyboardInterrupt, asyncio.CancelledError):
            persist_scrape_resume_pointer(data_dir, resume_next, browser=False)
            log.info(
                "Scrape interrupted — saving %d jobs collected so far.",
                len(all_jobs),
            )
            _interrupted = True

        log_run_summary(
            start_time, jobs_scraped_count, jobs_failed_count, all_jobs, output_file
        )

        return _interrupted
    finally:
        conn.close()


def main() -> None:
    """CLI entrypoint mirroring scrape.py."""
    parser = argparse.ArgumentParser(description="StackPulse fast (HTTP) job scraper")
    parser.add_argument(
        "--limit", type=int, default=JOBS_PER_QUERY, help="Max jobs per search query"
    )
    parser.add_argument(
        "--fresh", action="store_true", help="Re-scrape even already-seen URLs"
    )
    args = parser.parse_args()

    interrupted = asyncio.run(
        scrape_all_fast(limit_per_query=args.limit, fresh=args.fresh)
    )
    if interrupted:
        log.info("Interrupted by user. Exiting cleanly.")
        sys.exit(130)


if __name__ == "__main__":
    main()
