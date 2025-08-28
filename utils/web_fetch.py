import ssl
import aiohttp
import asyncio
import random
import requests
from bs4 import BeautifulSoup
from bs4.builder import ParserRejectedMarkup

import os, subprocess, time
from pathlib import Path
import fcntl  # Linux-only; Composer workers are Linux
from playwright.async_api import Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from .utils import _should_skip_blocked, _backoff_delay, _retry_after_seconds, _looks_internal_for_pw, _to_text, _is_probably_html

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.google.com/",
    "DNT": "1",  # Do Not Track
    "Connection": "keep-alive",
}

# Ensure browsers are stored on local disk, not GCS FUSE
BROWSERS_PATH = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/home/airflow/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH

_INSTALL_SENTINEL = Path(BROWSERS_PATH) / ".chromium_installed"
_INSTALL_LOCK = Path(BROWSERS_PATH) / ".install.lock"

_MAX_AIOHTTP_RETRIES = 2  # total tries = first + 2 retries on 429/5xx/timeout
_AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=45,       # full request budget
    connect=10,     # TCP connect budget
    sock_connect=10,
    sock_read=20,   # per-read budget
)
_DNS_TTL_SECS = 300

# ------ AIOHTTP Fetch ------

async def fetch_aiohttp(session, url: str) -> tuple[str, str | None]:
    await asyncio.sleep(random.uniform(0.05, 0.25))  # jitter
    attempt = 0
    while True:
        try:
            async with session.get(url, allow_redirects=True) as resp:
                status = resp.status

                # Fast terminal skips (no body read, no retries)
                if status in (401, 404, 410):
                    print(f"[AIOHTTP] {url} - {status} (terminal skip)")
                    return url, f"[SKIP:{status}]"

                # Success
                if 200 <= status < 300:
                    body = await resp.text()
                    print(f"[AIOHTTP] {url} - {status}")
                    print(f"[AIOHTTP] Fetched {len(body)} characters from {url}")
                    return url, body

                # Transient server or rate-limit errors → retry with backoff
                if status in (403, 429) or 500 <= status < 600:
                    # Honor Retry-After if present
                    ra = _retry_after_seconds(resp.headers) or 0.0
                    delay = max(ra, _backoff_delay(attempt))
                    if attempt < _MAX_AIOHTTP_RETRIES:
                        attempt += 1
                        print(f"[AIOHTTP-RETRY] {url} - {status}; sleeping {delay:.2f}s (attempt {attempt})")
                        await asyncio.sleep(delay)
                        continue
                    print(f"[AIOHTTP-GIVEUP] {url} - {status} after {attempt} retries")
                    return url, None  # let fallbacks try

                # Other client errors → skip (not valuable)
                if 400 <= status < 500:
                    print(f"[AIOHTTP] {url} - {status} (client error; skip)")
                    return url, f"[SKIP:{status}]"

                # Any other unhandled status: be conservative
                print(f"[AIOHTTP] {url} - {status} (unhandled; skip)")
                return url, f"[SKIP:{status}]"

        except aiohttp.ClientConnectorCertificateError as e:
            print(f"[AIOHTTP] {url} cert mismatch (terminal skip)")
            return url, "[SKIP:CERT_MISMATCH]"
        
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < _MAX_AIOHTTP_RETRIES:
                delay = _backoff_delay(attempt, base=0.5)
                attempt += 1
                print(f"[AIOHTTP-RETRY] {url}: {e.__class__.__name__}; sleeping {delay:.2f}s (attempt {attempt})")
                await asyncio.sleep(delay)
                continue
            print(f"[AIOHTTP-ERROR] {url}: {e.__class__.__name__}: {e} (giving up)")
            return url, None

        except Exception as e:
            print(f"[AIOHTTP-ERROR] {url}: {e.__class__.__name__}: {e} (giving up)")
            return url, None

#------ Requests Fetch ------

def fetch_requests(url: str) -> tuple[str, str | None]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=(5, 15), allow_redirects=True)
        status = r.status_code

        if status in (401, 404, 410):
            print(f"[REQUESTS] {url} - {status} (terminal skip)")
            return url, f"[SKIP:{status}]"

        if 200 <= status < 300:
            print(f"[REQUESTS] {url} - {status} ({len(r.text)} chars)")
            return url, r.text

        if status in (403, 429) or 500 <= status < 600:
            # one gentle retry so we don't block the event loop too long
            delay = _backoff_delay(0)
            print(f"[REQUESTS-RETRY] {url} - {status}; sleeping {delay:.2f}s")
            time.sleep(delay)
            r2 = requests.get(url, headers=HEADERS, timeout=(5, 15), allow_redirects=True)
            if 200 <= r2.status_code < 300:
                print(f"[REQUESTS] {url} - {r2.status_code} ({len(r2.text)} chars)")
                return url, r2.text
            print(f"[REQUESTS-GIVEUP] {url} - {r2.status_code}")
            return url, None

        # Other 4xx
        print(f"[REQUESTS] {url} - {status} (client error; skip)")
        return url, f"[SKIP:{status}]"

    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print(f"[REQUESTS-TIMEOUT] {url}: {e}")
        return url, None
    except Exception as e:
        print(f"[REQUESTS-ERROR] {url}: {e}")
        return url, None

# ------ Playwright Utilities/Fetch -----

PLAYWRIGHT_ENABLED = True  # circuit-breaker to avoid repeated failures

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

def _is_launch_error(msg: str) -> bool:
    # Messages that imply browser binaries missing / launch impossible
    return ("Executable doesn't exist" in msg) or ("Failed to launch" in msg) or ("browserType.launch" in msg)


async def fetch_playwright(url: str) -> tuple[str, str | None]:
    global PLAYWRIGHT_ENABLED
    if not PLAYWRIGHT_ENABLED:
        return url, None

    async def _try_launch():
        browser = None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                context = await browser.new_context()
                page = await context.new_page()
                resp = await page.goto(url, timeout=15000, wait_until="domcontentloaded")
                status = resp.status if resp else None

                if status in (404, 410):
                    print(f"[PLAYWRIGHT] {url} - {status} (terminal skip)")
                    return "[SKIP:404]"
                if status in (401, 403):
                    txt = (await page.content())[:16000]
                    if _should_skip_blocked(txt, len(txt)):
                        print(f"[PLAYWRIGHT] {url} - {status} (blocked; skip)")
                        return "[SKIP:403_BLOCKED]"

                html = await page.content()
                print(f"[PLAYWRIGHT] {url} - {status if status is not None else '?'}")
                return html

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

#------ Combined Fetch -----

async def fetch_all_pages(urls: list[str], max_concurrent: int, limit_per_host: int) -> dict[str, str]:
    results = {}

    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=limit_per_host, ttl_dns_cache=_DNS_TTL_SECS)

    async def fetch_with_semaphore(session, url):
        async with semaphore:
            return await fetch_aiohttp(session, url)
    
    try:
        async with aiohttp.ClientSession(connector=connector, timeout=_AIOHTTP_TIMEOUT, headers=HEADERS, trust_env=False) as session:
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
                if _looks_internal_for_pw(url_from_list):
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
            if _looks_internal_for_pw(url_from_list):
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

# ------ HTML/Text Extraction ------

def extract_visible_text(html: str) -> str:
    if not html or (isinstance(html, str) and html.startswith("[ERROR]")):
        return ""

    text = _to_text(html)
    if not text or not _is_probably_html(text):
        return ""
    
    try: 
        soup = BeautifulSoup(html, "html.parser")
        # Remove scripts and styles
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True)
    # If BeautifulSoup fails to parse, return empty
    except (ParserRejectedMarkup, AssertionError):
        return "" 
    except Exception:
        return ""  