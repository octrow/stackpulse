"""
Register Patchright as ``playwright.async_api`` before importing linkedin_scraper.

The ``linkedin-scraper`` package imports ``playwright.async_api``; Patchright exposes
the same API under ``patchright.async_api``. This module aliases the submodule so
BrowserManager and our scrapers use the Patchright driver.

Import and call :func:`install` at the top of any module that imports Playwright APIs.
"""

from __future__ import annotations

import sys

_installed = False


def install() -> None:
    """Map ``sys.modules['playwright.async_api']`` to Patchright (idempotent)."""
    global _installed
    if _installed:
        return
    import patchright.async_api as _async_api

    sys.modules["playwright.async_api"] = _async_api
    _installed = True
