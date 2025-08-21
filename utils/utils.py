from datetime import datetime, timezone
import time
import re, gzip, zlib
from typing import Union
import ipaddress
from urllib.parse import urlparse
import asyncio, random, re, time
from email.utils import parsedate_to_datetime

# ----- Web Fetch Utilities -----

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

# ----- URL Parsing Utilities -----

# Simple binary sniffers
_PDF_MAGIC  = b"%PDF-"
_ZIP_MAGIC  = b"PK\x03\x04"
_GZIP_MAGIC = b"\x1f\x8b"
_PNG_MAGIC  = b"\x89PNG\r\n\x1a\n"
_JPEG_MAGIC = b"\xff\xd8\xff"
_RIFF_MAGIC = b"RIFF"  # WEBP/AVI/etc heuristic
_CONTROL_BYTES = re.compile(rb"[\x00-\x08\x0B\x0C\x0E-\x1F]")

MAX_HTML_BYTES = 5 * 1024 * 1024  # 5MB cap to avoid OOM on huge pages

def _looks_binary(sample: bytes) -> bool:
    if sample.startswith((_PDF_MAGIC, _ZIP_MAGIC, _PNG_MAGIC, _JPEG_MAGIC, _RIFF_MAGIC)):
        return True
    if _CONTROL_BYTES.search(sample[:512]):
        return True
    return False

def _maybe_decompress(raw: bytes) -> bytes:
    # gzip
    if raw.startswith(_GZIP_MAGIC):
        try:
            return gzip.decompress(raw)
        except Exception:
            pass
    # zlib/deflate (auto header detection)
    try:
        return zlib.decompress(raw, wbits=zlib.MAX_WBITS | 32)
    except Exception:
        pass
    return raw

def _to_text(maybe_bytes: Union[str, bytes]) -> str:
    if isinstance(maybe_bytes, str):
        return maybe_bytes
    if not maybe_bytes:
        return ""

    raw = maybe_bytes[: MAX_HTML_BYTES + 1024]  # small read-ahead
    if _looks_binary(raw[:64]):
        return ""

    raw = _maybe_decompress(raw)
    if _looks_binary(raw[:64]):
        return ""

    # Size guard
    if len(raw) > MAX_HTML_BYTES:
        raw = raw[:MAX_HTML_BYTES]

    # Try a few common decodes
    for enc in ("utf-8", "latin-1", "utf-8-sig"):
        try:
            return raw.decode(enc, errors="replace")
        except Exception:
            continue
    return raw.decode("utf-8", errors="replace")  # last resort

def _is_probably_html(text: str) -> bool:
    sniff = text[:4096].lower()
    return ("<html" in sniff) or ("<!doctype html" in sniff) or ("<head" in sniff) or ("<body" in sniff)


# ----- URL Classification Utilities -----

def is_homepage(trimmed_page_url: str, site: str) -> bool:
    if not trimmed_page_url or not site:
        return False
    
    trimmed = trimmed_page_url.strip().lower().rstrip("/")
    site_clean = site.strip().lower().replace("www.", "")

    return trimmed == site_clean