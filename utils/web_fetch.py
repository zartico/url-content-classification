import ssl
import aiohttp
import asyncio
import random
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

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

async def fetch_aiohttp(session, url: str) -> tuple[str, str | None]:
    await asyncio.sleep(random.uniform(0.5, 1.5))  # jitter
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            print(f"[AIOHTTP] {url} - {resp.status}")
            content = await resp.text()
            print(f"[AIOHTTP] Fetched {len(content)} characters from {url}")
            return url, content
    except (aiohttp.ClientError, aiohttp.ClientConnectorError, asyncio.TimeoutError) as e:
        print(f"[AIOHTTP-TIMEOUT] {url}: {e}")
        return url, None
    except Exception as e:
        print(f"[AIOHTTP-ERROR] {url}: {e}")
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


async def fetch_playwright(url: str) -> tuple[str, str | None]:
    browser = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            response = await page.goto(url, timeout=15000)
            content = await page.content()
            status = response.status if response else "?"
            print(f"[PLAYWRIGHT] {url} - {status}")
            print(f"[PLAYWRIGHT] Fetched {len(content)} characters from {url}")
            return url, content
    except PlaywrightTimeoutError:
        print(f"[PLAYWRIGHT-TIMEOUT] {url}")
    except Exception as e:
        print(f"[PLAYWRIGHT-ERROR] {url}: {e}")
    finally:
        if browser:
            try:
                await browser.close()
                print(f"[PLAYWRIGHT] Browser closed for {url}")
            except Exception as e:
                print(f"[PLAYWRIGHT-CLEANUP-ERROR] {url}: {e}")
    
    return url, None


async def fetch_all_pages(urls: list[str], max_concurrent: int) -> dict[str, str]:
    results = {}

    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=2)

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
    soup = BeautifulSoup(html, "html.parser")
    # Remove scripts and styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)
