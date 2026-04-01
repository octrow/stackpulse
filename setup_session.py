"""
Run this ONCE to log in to LinkedIn and save the session.
After that, scrape.py will reuse session.json without re-logging in.

Usage:
    python setup_session.py
"""

import asyncio
import os

from dotenv import load_dotenv
from playwright.async_api import Page

from config import SESSION_FILE

load_dotenv()

LINKEDIN_EMAIL = os.getenv("LINKEDIN_EMAIL")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")


async def main() -> None:
    """Log in to LinkedIn and persist the browser session."""
    # Import here so missing playwright doesn't break the import at module level
    from linkedin_scraper import BrowserManager

    print("Starting browser (non-headless so you can log in)...")

    async with BrowserManager(headless=False) as browser:
        page = browser.page

        if LINKEDIN_EMAIL and LINKEDIN_PASSWORD:
            print(f"Logging in as {LINKEDIN_EMAIL} via credentials...")
            try:
                from linkedin_scraper import login_with_credentials

                await login_with_credentials(
                    page,
                    email=LINKEDIN_EMAIL,
                    password=LINKEDIN_PASSWORD,
                )
                print("Logged in successfully.")
            except (TimeoutError, RuntimeError, ValueError) as error:
                print(
                    "Credential login failed "
                    f"({type(error).__name__}: {error}), falling back to manual login."
                )
                await _manual_login(page)
        else:
            print("No credentials in .env — opening login page for manual login.")
            await _manual_login(page)

        print(f"Saving session to {SESSION_FILE} ...")
        await browser.save_session(SESSION_FILE)
        print("Done. You can now run: python scrape.py")


async def _manual_login(page: Page) -> None:
    """Navigate to the LinkedIn login page and wait for the user to complete login."""
    await page.goto("https://www.linkedin.com/login")
    print("\nPlease log in manually in the browser window.")
    print("Waiting up to 5 minutes for you to complete login...")

    try:
        from linkedin_scraper import wait_for_manual_login

        await wait_for_manual_login(page, timeout=300_000)
    except ImportError:
        # Fallback: wait until the feed URL appears
        await page.wait_for_url("**/feed/**", timeout=300_000)

    print("Login detected.")


if __name__ == "__main__":
    asyncio.run(main())
