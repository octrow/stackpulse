"""
Direct Playwright scraper for LinkedIn job pages.
Replaces the broken linkedin_scraper.JobScraper which never waits
for the React SPA to render content (it only waits for domcontentloaded).
"""

import asyncio
import logging
import pathlib
from typing import Callable

from playwright.async_api import (
    Error as PlaywrightError,
    Page,
    TimeoutError as PWTimeout,
)

from config import (
    OUTPUT_DIR,
    PAGE_LOAD_TIMEOUT_MS,
    H1_WAIT_TIMEOUT_MS,
    BUTTON_CLICK_TIMEOUT_MS,
    POST_CLICK_SETTLE_SECONDS,
    POST_EXPAND_SETTLE_SECONDS,
    DEBUG_HTML_SNIPPET_CHARS,
    DESCRIPTION_MIN_CHARS,
)

logger = logging.getLogger(__name__)

# LinkedIn "expand description" button labels to try in order
_EXPAND_BUTTON_LABELS = ["Show more", "See more", "…see more"]

# CSS selectors tried in order for each field (most-specific → generic fallback)
_TITLE_SELECTORS: list[str] = [
    "h1.t-24",
    "h1.jobs-unified-top-card__job-title",
    "h1",
]
_COMPANY_SELECTORS: list[str] = [
    "a.app-aware-link[href*='/company/']",
    ".jobs-unified-top-card__company-name a",
    ".topcard__org-name-link",
    "a[href*='/company/']",
]
_LOCATION_SELECTORS: list[str] = [
    ".jobs-unified-top-card__bullet",
    ".topcard__flavor--bullet",
    "span.tvm__text",
    "span",  # generic fallback
]
_POSTED_DATE_SELECTORS: list[str] = [
    "span.jobs-unified-top-card__posted-date",
    ".topcard__flavor--metadata",
    "span",
]
_APPLICANT_SELECTORS: list[str] = [
    ".jobs-unified-top-card__applicant-count",
    "span.num-applicants__caption",
    "span",
    "div",
]
_DESCRIPTION_SELECTORS: list[str] = [
    "div.jobs-description-content__text",
    "div#job-details",
    "div.jobs-description__content",
    "div[class*='description'] article",
    "article",
]

_LINKEDIN_BASE_URL = "https://www.linkedin.com"


async def scrape_job(page: Page, url: str) -> dict:
    """Navigate to a LinkedIn job posting and extract all fields.

    Returns a dict with the same keys as the library's Job model.
    On render failure, saves a screenshot + HTML snippet to OUTPUT_DIR/debug/.
    """
    await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)

    # Wait until at least the job title h1 appears (SPA renders async)
    try:
        await page.wait_for_selector("h1", timeout=H1_WAIT_TIMEOUT_MS)
    except PWTimeout:
        logger.warning(f"h1 never appeared on {url}")
        await _save_debug_snapshot(page, url)

    # Expand truncated description ("Show more" / "See more")
    for button_label in _EXPAND_BUTTON_LABELS:
        try:
            button = page.locator(f"button:has-text('{button_label}')").first
            if await button.count() > 0:
                await button.click(timeout=BUTTON_CLICK_TIMEOUT_MS)
                await asyncio.sleep(POST_CLICK_SETTLE_SECONDS)
        except (PWTimeout, PlaywrightError) as error:
            logger.debug(
                "Expand button click failed for '%s' on %s (%s): %s",
                button_label,
                url,
                type(error).__name__,
                error,
            )

    # Small settle wait after any click
    await asyncio.sleep(POST_EXPAND_SETTLE_SECONDS)

    job_title = await _get_title(page)
    company, company_url = await _get_company(page)
    location = await _get_location(page)
    posted_date = await _get_posted_date(page)
    applicant_count = await _get_applicants(page)
    description = await _get_description(page)

    return {
        "linkedin_url": url,
        "job_title": job_title,
        "company": company,
        "company_linkedin_url": company_url,
        "location": location,
        "posted_date": posted_date,
        "applicant_count": applicant_count,
        "job_description": description,
        "benefits": None,
    }


async def _save_debug_snapshot(page: Page, url: str) -> None:
    """Save a screenshot and HTML snippet for a page that failed to render."""
    debug_dir = pathlib.Path(OUTPUT_DIR) / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    job_id_slug = url.rstrip("/").split("/")[-1]
    await page.screenshot(path=str(debug_dir / f"{job_id_slug}.png"), full_page=True)
    html_content = await page.content()
    (debug_dir / f"{job_id_slug}.html").write_text(
        html_content[:DEBUG_HTML_SNIPPET_CHARS], encoding="utf-8"
    )
    page_title = await page.title()
    print(
        f"  DEBUG: title='{page_title}' | "
        f"screenshot+html saved to {debug_dir}/{job_id_slug}.*"
    )


async def _extract_text(locator, timeout_ms: int = 3_000) -> str | None:
    """Return stripped inner text of the first match, or None on failure."""
    try:
        text = await locator.first.inner_text(timeout=timeout_ms)
        return text.strip() or None
    except (PWTimeout, PlaywrightError):
        return None


async def _find_text_matching(
    page: Page,
    selectors: list[str],
    predicate: Callable[[str], bool],
) -> str | None:
    """Try each selector in order, returning the first element text that satisfies predicate.

    Iterates all elements for each selector before moving to the next.
    Returns None if no match is found across all selectors.
    """
    for selector in selectors:
        try:
            elements = await page.locator(selector).all()
            for element in elements:
                text = (await element.inner_text()).strip()
                if text and predicate(text):
                    return text
        except (PWTimeout, PlaywrightError) as error:
            logger.debug(
                "Selector '%s' failed during text match (%s): %s",
                selector,
                type(error).__name__,
                error,
            )
            continue
    return None


def _normalise_company_url(href: str) -> str | None:
    """Strip query parameters and ensure the URL is absolute."""
    if not href:
        return None
    href = href.split("?")[0]
    if href and not href.startswith("http"):
        href = _LINKEDIN_BASE_URL + href
    return href or None


async def _get_title(page: Page) -> str | None:
    """Extract the job title from the top card."""
    for selector in _TITLE_SELECTORS:
        text = await _extract_text(page.locator(selector))
        if text:
            return text
    return None


async def _get_company(page: Page) -> tuple[str | None, str | None]:
    """Extract company name and LinkedIn URL from the top card."""
    for selector in _COMPANY_SELECTORS:
        try:
            links = await page.locator(selector).all()
            for link in links:
                text = (await link.inner_text()).strip()
                if text and len(text) > 1 and "logo" not in text.lower():
                    href = await link.get_attribute("href") or ""
                    return text, _normalise_company_url(href)
        except (PWTimeout, PlaywrightError) as error:
            logger.debug(
                "Selector '%s' failed during text match (%s): %s",
                selector,
                type(error).__name__,
                error,
            )
            continue
    return None, None


async def _get_location(page: Page) -> str | None:
    """Extract the job location string."""

    def _is_location(text: str) -> bool:
        return 3 < len(text) < 80 and (
            "," in text or "Remote" in text or "Hybrid" in text
        )

    return await _find_text_matching(page, _LOCATION_SELECTORS, _is_location)


async def _get_posted_date(page: Page) -> str | None:
    """Extract the relative posting date (e.g. '2 days ago')."""
    _DATE_KEYWORDS = ("ago", "hour", "day", "week", "month", "reposted")

    def _is_posted_date(text: str) -> bool:
        return len(text) < 60 and any(
            keyword in text.lower() for keyword in _DATE_KEYWORDS
        )

    return await _find_text_matching(page, _POSTED_DATE_SELECTORS, _is_posted_date)


async def _get_applicants(page: Page) -> str | None:
    """Extract the applicant count string."""
    _APPLICANT_KEYWORDS = ("applicant", "people clicked", "applied")

    def _is_applicant_count(text: str) -> bool:
        return len(text) < 80 and any(
            keyword in text.lower() for keyword in _APPLICANT_KEYWORDS
        )

    return await _find_text_matching(page, _APPLICANT_SELECTORS, _is_applicant_count)


async def _get_description(page: Page) -> str | None:
    """Extract the full job description text.

    Tries specific containers first, then falls back to an 'About the job' heading search.
    """
    for selector in _DESCRIPTION_SELECTORS:
        try:
            element = page.locator(selector).first
            if await element.count() > 0:
                text = (await element.inner_text()).strip()
                if text and len(text) > DESCRIPTION_MIN_CHARS:
                    return text
        except (PWTimeout, PlaywrightError) as error:
            logger.debug(
                "Selector '%s' failed during text match (%s): %s",
                selector,
                type(error).__name__,
                error,
            )
            continue

    # Last resort: find "About the job" section and grab surrounding text
    try:
        heading = page.locator("h2").filter(has_text="About the job").first
        if await heading.count() > 0:
            parent = heading.locator("xpath=ancestor::div[3]")
            if await parent.count() > 0:
                text = (await parent.inner_text()).strip()
                if text and len(text) > DESCRIPTION_MIN_CHARS:
                    return text
    except (PWTimeout, PlaywrightError) as error:
        logger.debug(
            "Fallback 'About the job' extraction failed (%s): %s",
            type(error).__name__,
            error,
        )

    return None
