import ssl
import aiohttp
import asyncio
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )
}

# Fetch webpage content asynchronously with fallback for SSL
async def fetch_single_page(session, url):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as response:
            print(f"[DEBUG] URL fetched: {url} - Status: {response.status}")

            if response.status == 403 or response.status == 410:
                # Retry with headers if forbidden or gone
                async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=8)) as retry_response:
                    print(f"[DEBUG] Header retry for {url} - Status: {retry_response.status}")
                    if retry_response.status != 200:
                        return url, f"[ERROR {retry_response.status}]"
                    html = await retry_response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    text = soup.get_text(separator=' ', strip=True)
                    return url, text
            
            elif response.status != 200:
                return url, ""
            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(separator=' ', strip=True)
            print(f"[DEBUG] Word count: {len(text.split())}")
            return url, text
        
    except aiohttp.ClientConnectorCertificateError:
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8), ssl=ssl_context) as response:
                print(f"[DEBUG] SSL fallback fetch: {url} - Status: {response.status}")
                if response.status != 200:
                    return url, ""
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(separator=' ', strip=True)
                print(f"[DEBUG] Word count (fallback): {len(text.split())}")
                return url, text
        except Exception as e:
            print(f"[ERROR] Retry failed for {url}: {e}")
            return url, ""
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
        return url, ""

async def fetch_all_pages(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_single_page(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return dict(results)