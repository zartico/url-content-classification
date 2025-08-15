from datetime import datetime, timezone
import time
import re
import ipaddress
from urllib.parse import urlparse
import asyncio, random, re, time
from email.utils import parsedate_to_datetime

# Common “blocked” signatures (lowercased match on a short sample)
_BLOCK_PATTERNS = (
    "access denied", "request blocked", "forbidden", "permission denied",
    "captcha", "verify you are human", "attention required",
    "cloudflare", "akamai", "incapsula", "perimeterx", "sucuri",
)

def _should_skip_blocked(sample_text: str, sample_len: int) -> bool:
    t = sample_text.lower()
    if sample_len < 10_000:
        return True
    return any(p in t for p in _BLOCK_PATTERNS)

def _backoff_delay(attempt: int, base: float = 0.75) -> float:
    # exp backoff with jitter
    return base * (2 ** attempt) * random.uniform(0.5, 1.5)

def _retry_after_seconds(resp_headers) -> float | None:
    ra = resp_headers.get("Retry-After")
    if not ra:
        return None
    try:
        # integer seconds
        return float(ra)
    except ValueError:
        try:
            # HTTP-date
            dt = parsedate_to_datetime(ra)
            return max(0.0, (dt - dt.now(dt.tzinfo)).total_seconds())
        except Exception:
            return None

# obvious non-publics
def _looks_internal_for_pw(u: str) -> bool:
    from urllib.parse import urlparse
    try:
        h = (urlparse(u).hostname or "").lower()
    except Exception:
        return True
    if not h: return True
    tld = h.rsplit(".", 1)[-1]
    if tld in {"test","local","localhost","invalid","example"}: return True
    if h.endswith(".local.simpleviewcms.com") or h.endswith(".cms30.localhost"): return True
    return False


def is_homepage(trimmed_page_url: str, site: str) -> bool:
    if not trimmed_page_url or not site:
        return False
    
    trimmed = trimmed_page_url.strip().lower().rstrip("/")
    site_clean = site.strip().lower().replace("www.", "")

    return trimmed == site_clean