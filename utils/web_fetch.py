import ssl
import aiohttp
import asyncio
import random
import requests
from playwright.async_api import async_playwright

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

async def fetch_aiohttp(session, url):
    await asyncio.sleep(random.uniform(0.5, 2.0))
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            print(f"[AIOHTTP] {url} - {resp.status}")
            if resp.status == 200:
                html = await resp.text()
                return url, html
            return url, None  # trigger fallback
    except Exception as e:
        print(f"[AIOHTTP-ERROR] {url}: {e}")
        return url, None

def fetch_requests(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        print(f"[REQUESTS] {url} - {r.status_code}")
        if r.status_code == 200:
            return url, r.text
        return url, None
    except Exception as e:
        print(f"[REQUESTS-ERROR] {url}: {e}")
        return url, None

def fetch_playwright(url):
    try:
        with async_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.goto(url, timeout=15000)
            content = page.content()
            print(f"[PLAYWRIGHT] {url} - OK")
            browser.close()
            return url, content
    except Exception as e:
        print(f"[PLAYWRIGHT-ERROR] {url}: {e}")
        return url, None

async def fetch_all_pages(urls):
    results = {}

    async with aiohttp.ClientSession() as session:
        aio_tasks = [fetch_aiohttp(session, url) for url in urls]
        aio_results = await asyncio.gather(*aio_tasks)

    for url, html in aio_results:
        if html:
            results[url] = html
            continue

        # Try requests
        url, html = fetch_requests(url)
        if html:
            results[url] = html
            continue

        # Try Playwright
        url, html = fetch_playwright(url)
        if html:
            results[url] = html
        else:
            results[url] = "[ERROR] All methods failed"

    return results
