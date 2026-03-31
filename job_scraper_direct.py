"""
Direct Playwright scraper for LinkedIn job pages.
Replaces the broken linkedin_scraper.JobScraper which never waits
for the React SPA to render content (it only waits for domcontentloaded).
"""

import asyncio
import logging
from typing import Optional
from playwright.async_api import Page, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)


async def scrape_job(page: Page, url: str) -> dict:
    """
    Navigate to a LinkedIn job posting and extract all fields.
    Returns a dict with the same keys as the library's Job model.
    """
    await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    # Wait until at least the job title h1 appears (SPA renders async)
    try:
        await page.wait_for_selector("h1", timeout=15_000)
    except PWTimeout:
        logger.warning(f"h1 never appeared on {url}")
        # Debug: save screenshot + page title so we can see what LinkedIn returned
        import pathlib
        debug_dir = pathlib.Path("data/debug")
        debug_dir.mkdir(parents=True, exist_ok=True)
        slug = url.rstrip("/").split("/")[-1]
        await page.screenshot(path=str(debug_dir / f"{slug}.png"), full_page=True)
        html_snippet = await page.content()
        (debug_dir / f"{slug}.html").write_text(html_snippet[:8000], encoding="utf-8")
        print(f"  DEBUG: title='{await page.title()}' | screenshot+html saved to data/debug/{slug}.*")

    # Expand truncated description ("Show more" / "See more")
    for btn_text in ["Show more", "See more", "…see more"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}')").first
            if await btn.count() > 0:
                await btn.click(timeout=3_000)
                await asyncio.sleep(0.5)
        except Exception:
            pass

    # Small settle wait after any click
    await asyncio.sleep(1)

    job_title    = await _get_title(page)
    company, company_url = await _get_company(page)
    location     = await _get_location(page)
    posted_date  = await _get_posted_date(page)
    applicants   = await _get_applicants(page)
    description  = await _get_description(page)

    return {
        "linkedin_url":         url,
        "job_title":            job_title,
        "company":              company,
        "company_linkedin_url": company_url,
        "location":             location,
        "posted_date":          posted_date,
        "applicant_count":      applicants,
        "job_description":      description,
        "benefits":             None,
    }


async def _text(locator, timeout=3_000) -> Optional[str]:
    try:
        t = await locator.first.inner_text(timeout=timeout)
        return t.strip() or None
    except Exception:
        return None


async def _get_title(page: Page) -> Optional[str]:
    # Authenticated view
    for sel in [
        "h1.t-24",
        "h1.jobs-unified-top-card__job-title",
        "h1",
    ]:
        t = await _text(page.locator(sel))
        if t:
            return t
    return None


async def _get_company(page: Page) -> tuple[Optional[str], Optional[str]]:
    # The company link sits near the title in the top card
    for sel in [
        "a.app-aware-link[href*='/company/']",
        ".jobs-unified-top-card__company-name a",
        ".topcard__org-name-link",
        "a[href*='/company/']",
    ]:
        try:
            links = await page.locator(sel).all()
            for link in links:
                text = (await link.inner_text()).strip()
                if text and len(text) > 1 and "logo" not in text.lower():
                    href = await link.get_attribute("href") or ""
                    href = href.split("?")[0]
                    if href and not href.startswith("http"):
                        href = "https://www.linkedin.com" + href
                    return text, href or None
        except Exception:
            continue
    return None, None


async def _get_location(page: Page) -> Optional[str]:
    for sel in [
        ".jobs-unified-top-card__bullet",
        ".topcard__flavor--bullet",
        "span.tvm__text",
        # Generic: look for spans near h1 that look like a city
        "span",
    ]:
        try:
            elems = await page.locator(sel).all()
            for elem in elems:
                t = (await elem.inner_text()).strip()
                if t and 3 < len(t) < 80 and ("," in t or "Remote" in t or "Hybrid" in t):
                    return t
        except Exception:
            continue
    return None


async def _get_posted_date(page: Page) -> Optional[str]:
    for sel in [
        "span.jobs-unified-top-card__posted-date",
        ".topcard__flavor--metadata",
        "span",
    ]:
        try:
            elems = await page.locator(sel).all()
            for elem in elems:
                t = (await elem.inner_text()).strip()
                if t and len(t) < 60:
                    tl = t.lower()
                    if any(k in tl for k in ("ago", "hour", "day", "week", "month", "reposted")):
                        return t
        except Exception:
            continue
    return None


async def _get_applicants(page: Page) -> Optional[str]:
    for sel in [
        ".jobs-unified-top-card__applicant-count",
        "span.num-applicants__caption",
        "span",
        "div",
    ]:
        try:
            elems = await page.locator(sel).all()
            for elem in elems:
                t = (await elem.inner_text()).strip()
                if t and len(t) < 80:
                    tl = t.lower()
                    if any(k in tl for k in ("applicant", "people clicked", "applied")):
                        return t
        except Exception:
            continue
    return None


async def _get_description(page: Page) -> Optional[str]:
    # Try specific containers first, then fall back to article
    for sel in [
        "div.jobs-description-content__text",
        "div#job-details",
        "div.jobs-description__content",
        "div[class*='description'] article",
        "article",
    ]:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0:
                t = (await elem.inner_text()).strip()
                if t and len(t) > 100:
                    return t
        except Exception:
            continue

    # Last resort: find "About the job" section and grab surrounding text
    try:
        heading = page.locator("h2").filter(has_text="About the job").first
        if await heading.count() > 0:
            # Walk up to a containing div with significant text
            parent = heading.locator("xpath=ancestor::div[3]")
            if await parent.count() > 0:
                t = (await parent.inner_text()).strip()
                if t and len(t) > 100:
                    return t
    except Exception:
        pass

    return None
