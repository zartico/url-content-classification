import ssl
import aiohttp
import asyncio
import random
import requests
from bs4 import BeautifulSoup

import os, subprocess, time
from pathlib import Path
import fcntl  # Linux-only; Composer workers are Linux
from playwright.async_api import Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from .utils import looks_internal_for_pw

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.google.com/",
    "DNT": "1",  # Do Not Track
    "Connection": "keep-alive",
}

# Ensure browsers are stored on local disk, not GCS FUSE
BROWSERS_PATH = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/home/airflow/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH

_INSTALL_SENTINEL = Path(BROWSERS_PATH) / ".chromium_installed"
_INSTALL_LOCK = Path(BROWSERS_PATH) / ".install.lock"

def ensure_playwright_browsers_installed() -> bool:
    """
    Ensure Chromium is installed for Playwright on this worker.
    Uses a file lock so only one process downloads at a time.
    Returns True if browsers are installed (now or already), else False.
    """
    try:
        if _INSTALL_SENTINEL.exists():
            return True

        _INSTALL_SENTINEL.parent.mkdir(parents=True, exist_ok=True)
        with open(_INSTALL_LOCK, "w") as lf:
            # Exclusive lock so concurrent tasks don't both download
            fcntl.flock(lf, fcntl.LOCK_EX)
            # Another process may have finished while we waited
            if _INSTALL_SENTINEL.exists():
                fcntl.flock(lf, fcntl.LOCK_UN)
                return True

            print("[PLAYWRIGHT-INSTALL] Downloading Chromium to", BROWSERS_PATH)
            # Do NOT use --with-deps inside Composer; not allowed to apt-get
            subprocess.run(
                ["python", "-m", "playwright", "install", "chromium"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            _INSTALL_SENTINEL.touch()
            fcntl.flock(lf, fcntl.LOCK_UN)
            print("[PLAYWRIGHT-INSTALL] Completed")
            return True
    except subprocess.CalledProcessError as e:
        print("[PLAYWRIGHT-INSTALL-ERROR] Installer failed:\n", e.stdout)
    except Exception as e:
        print(f"[PLAYWRIGHT-INSTALL-ERROR] {e}")
    return False

async def fetch_aiohttp(session, url: str) -> tuple[str, str | None]:
    await asyncio.sleep(random.uniform(0.05, 0.25))  # jitter
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            print(f"[AIOHTTP] {url} - {resp.status}")
            content = await resp.text()
            print(f"[AIOHTTP] Fetched {len(content)} characters from {url}")
            return url, content
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"[AIOHTTP-ERROR] {url}: {e.__class__.__name__}: {e}")
        return url, None
    except Exception as e:
        print(f"[AIOHTTP-ERROR] {url}:{e.__class__.__name__}: {e}")
        return url, None


def fetch_requests(url: str) -> tuple[str, str | None]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        print(f"[REQUESTS] {url} - {r.status_code}")
        print(f"[REQUESTS] {url} fetched with {len(r.text)} characters")
        return url, r.text
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print(f"[REQUESTS-TIMEOUT] {url}: {e}")
        return url, None
    except Exception as e:
        print(f"[REQUESTS-ERROR] {url}: {e}")
        return url, None

PLAYWRIGHT_ENABLED = True  # circuit-breaker to avoid repeated failures

def _is_launch_error(msg: str) -> bool:
    # Messages that imply browser binaries missing / launch impossible
    return ("Executable doesn't exist" in msg) or ("Failed to launch" in msg) or ("browserType.launch" in msg)


async def fetch_playwright(url: str) -> tuple[str, str | None]:
    global PLAYWRIGHT_ENABLED
    if not PLAYWRIGHT_ENABLED:
        return url, None

    async def _try_launch():
        from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
        browser = None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                context = await browser.new_context()
                page = await context.new_page()
                response = await page.goto(url, timeout=15000)
                content = await page.content()
                status = response.status if response else "?"
                print(f"[PLAYWRIGHT] {url} - {status}")
                return content
        except PlaywrightTimeoutError:
            print(f"[PLAYWRIGHT-TIMEOUT] {url}")
            return None
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception as e:
                    print(f"[PLAYWRIGHT-CLEANUP] {url}: {e}")

    # First attempt: will fail fast if browsers not present
    try:
        content = await _try_launch()
        return (url, content) if content else (url, None)
    except Exception as e:
        msg = str(e)
        needs_install = ("Executable doesn't exist" in msg) or ("playwright install" in msg)
        if not needs_install:
            print(f"[PLAYWRIGHT-ERROR] {url}: {e.__class__.__name__}: {msg}")
            return url, None

    # On-demand install (one per worker), then retry once
    if ensure_playwright_browsers_installed():
        try:
            content = await _try_launch()
            return (url, content) if content else (url, None)
        except Exception as e:
            msg = str(e)
            # Disable only if it's still a launch/executable problem
            if _is_launch_error(msg):
                PLAYWRIGHT_ENABLED = False
                print("[PLAYWRIGHT-DISABLED] Browser launch failure after install; disabling fallback.")
            else:
                print(f"[PLAYWRIGHT-ERROR-AFTER-INSTALL] {url}: {e.__class__.__name__}: {msg}")
            return url, None

    # Disable for the rest of the run on this process
    PLAYWRIGHT_ENABLED = False
    print("[PLAYWRIGHT-DISABLED] Could not launch even after install; disabling fallback.")
    return url, None

# async def fetch_playwright(url: str) -> tuple[str, str | None]:
#     browser = None
#     try:
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=True)
#             context = await browser.new_context()
#             page = await context.new_page()
#             response = await page.goto(url, timeout=15000)
#             content = await page.content()
#             status = response.status if response else "?"
#             print(f"[PLAYWRIGHT] {url} - {status}")
#             print(f"[PLAYWRIGHT] Fetched {len(content)} characters from {url}")
#             return url, content
#     except PlaywrightTimeoutError:
#         print(f"[PLAYWRIGHT-TIMEOUT] {url}")
#     except Exception as e:
#         print(f"[PLAYWRIGHT-ERROR] {url}: {e}")
#     finally:
#         if browser:
#             try:
#                 await browser.close()
#                 print(f"[PLAYWRIGHT] Browser closed for {url}")
#             except Exception as e:
#                 print(f"[PLAYWRIGHT-CLEANUP-ERROR] {url}: {e}")
    
#     return url, None


async def fetch_all_pages(urls: list[str], max_concurrent: int, limit_per_host: int) -> dict[str, str]:
    results = {}

    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=limit_per_host)

    async def fetch_with_semaphore(session, url):
        async with semaphore:
            return await fetch_aiohttp(session, url)
    
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            aio_tasks = [fetch_with_semaphore(session, url) for url in urls]
            aio_results = await asyncio.gather(*aio_tasks, return_exceptions=True)

        for i, result in enumerate(aio_results):
            url_from_list = urls[i]

            if isinstance(result, Exception):
                print(f"[FETCH-ERROR] Exception in async aiohttp gather for {url_from_list}: {result}")
                url, html = fetch_requests(url_from_list)
                if html:
                    results[url] = html
                    continue
                print(f"[FETCH] Requests failed for {url_from_list}, trying Playwright")

                # Skip playwright for internal/non-public domains
                if looks_internal_for_pw(url_from_list):
                    results[url_from_list] = "[ERROR] Internal/non-public domain"
                    continue            

                url, html = await fetch_playwright(url_from_list)
                results[url] = html if html else "[ERROR] All methods failed"
                continue

            url, html = result
            if html:
                results[url] = html
                continue

            # Try requests
            print(f"[FETCH] AIOHTTP failed for {url}, trying requests")
            url, html = fetch_requests(url)
            if html:
                results[url] = html
                continue

            # Skip playwright for internal/non-public domains
            if looks_internal_for_pw(url_from_list):
                results[url_from_list] = "[ERROR] Internal/non-public domain"
                continue 

            # Try Playwright
            print(f"[FETCH] Requests failed for {url}, trying Playwright")
            url, html = await fetch_playwright(url)
            results[url] = html if html else "[ERROR] All methods failed"

    except Exception as e:
        print(f"[FETCH-ALL-ERROR] Unexpected error in fetch_all_pages: {e}")
        import traceback
        print(traceback.format_exc())
        for url in urls:
            if url not in results:
                results[url] = "[ERROR] Critical fetch failure"

    print(f"[FETCH] Completed. Fetched content for {len(results)} URLs")
    return results

def extract_visible_text(html: str) -> str:
    if not html or (isinstance(html, str) and html.startswith("[ERROR]")):
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # Remove scripts and styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)
