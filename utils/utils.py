from datetime import datetime, timezone
import time
import re
import ipaddress
from urllib.parse import urlparse

# obvious non-publics
def looks_internal_for_pw(u: str) -> bool:
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