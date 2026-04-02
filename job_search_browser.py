"""
Paginated LinkedIn job search in the browser.

The linkedin_scraper.JobSearchScraper scrolls the *window* a few times; LinkedIn's
job results live in a scrollable column, so body scrollHeight often stops changing
after one pass — yielding only a handful of URLs. We paginate with the same
`start=` query parameter the LinkedIn UI uses (e.g. start=25, 50, …).
"""

from __future__ import annotations

import patchright_shim

patchright_shim.install()

import asyncio
import logging
from urllib.parse import urlencode

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PWTimeout

from config import (
    DELAY_BETWEEN_SEARCH_PAGES,
    JOB_SEARCH_MAX_START,
    JOB_SEARCH_PAGE_SIZE,
    PAGE_LOAD_TIMEOUT_MS,
)

from linkedin_scraper.core import detect_rate_limit

log = logging.getLogger("scraper")


def _normalize_job_url(href: str | None) -> str | None:
    if not href or "/jobs/view/" not in href:
        return None
    clean = href.split("?")[0]
    if not clean.startswith("http"):
        clean = f"https://www.linkedin.com{clean}"
    return clean


async def search_job_urls_paginated(
    page: Page,
    keywords: str,
    location: str,
    limit: int,
) -> list[str]:
    """Collect up to ``limit`` unique job posting URLs using ``start=`` pagination."""
    base = "https://www.linkedin.com/jobs/search/"
    collected: list[str] = []
    seen: set[str] = set()
    start = 0

    while len(collected) < limit and start < JOB_SEARCH_MAX_START:
        params: dict[str, str | int] = {
            "keywords": keywords,
            "location": location,
            "start": start,
        }
        url = f"{base}?{urlencode(params)}"
        log.debug("Job search GET start=%s", start)

        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)
        await detect_rate_limit(page)

        try:
            await page.wait_for_selector(
                'a[href*="/jobs/view/"]',
                timeout=10_000,
            )
        except PWTimeout:
            log.warning(
                "No job links on search page (start=%s) — may be logged out or blocked.",
                start,
            )
            break

        await asyncio.sleep(0.5)
        links = await page.locator('a[href*="/jobs/view/"]').all()

        new_on_page = 0
        for link in links:
            if len(collected) >= limit:
                break
            try:
                href = await link.get_attribute("href")
            except PlaywrightError:
                continue
            normalized = _normalize_job_url(href)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            collected.append(normalized)
            new_on_page += 1

        if new_on_page == 0:
            log.debug(
                "Pagination stop at start=%s — no new job URLs (have %s total).",
                start,
                len(collected),
            )
            break

        start += JOB_SEARCH_PAGE_SIZE
        await asyncio.sleep(DELAY_BETWEEN_SEARCH_PAGES)

    return collected[:limit]
