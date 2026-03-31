"""
Run this ONCE to log in to LinkedIn and save the session.
After that, scrape.py will reuse session.json without re-logging in.

Usage:
    python setup_session.py
"""

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

EMAIL = os.getenv("LINKEDIN_EMAIL")
PASSWORD = os.getenv("LINKEDIN_PASSWORD")


async def main():
    # Import here so missing playwright doesn't break the import at module level
    from linkedin_scraper import BrowserManager

    print("Starting browser (non-headless so you can log in)...")

    async with BrowserManager(headless=False) as browser:
        page = browser.page

        if EMAIL and PASSWORD:
            print(f"Logging in as {EMAIL} via credentials...")
            try:
                from linkedin_scraper import login_with_credentials
                await login_with_credentials(page, email=EMAIL, password=PASSWORD)
                print("Logged in successfully.")
            except Exception as e:
                print(f"Credential login failed ({e}), falling back to manual login.")
                await _manual_login(page)
        else:
            print("No credentials in .env — opening login page for manual login.")
            await _manual_login(page)

        print("Saving session to session.json ...")
        await browser.save_session("session.json")
        print("Done. You can now run: python scrape.py")


async def _manual_login(page):
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
