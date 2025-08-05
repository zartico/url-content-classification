from airflow.models import Variable
from datetime import datetime, timezone
import time

# === Quota Config ===
DAILY_LIMIT = 750000  # ~94% of 800,000 GCP API daily limit
MINUTE_LIMIT = 550  # ~94% of 600 API calls per minute

# === Variable Keys ===
VAR_DAY_COUNT = "nlp_day_count"
VAR_DAY_START = "nlp_day_start"
VAR_MIN_COUNT = "nlp_minute_count"
VAR_MIN_START = "nlp_minute_start"


def _get_airflow_var(key, default):
    try:
        return Variable.get(key, default_var=default)
    except Exception:
        return default


def _set_airflow_var(key, value):
    Variable.set(key, value)


def reset_if_new_day():
    today = str(datetime.now(timezone.utc).date())
    stored_day = _get_airflow_var(VAR_DAY_START, today)
    if stored_day != today:
        _set_airflow_var(VAR_DAY_COUNT, 0)
        _set_airflow_var(VAR_DAY_START, today)


def reset_if_new_minute():
    now_ts = time.time()
    start_ts = float(_get_airflow_var(VAR_MIN_START, now_ts))
    if now_ts - start_ts > 60:
        _set_airflow_var(VAR_MIN_COUNT, 0)
        _set_airflow_var(VAR_MIN_START, now_ts)


def check_and_increment_quota():
    reset_if_new_day()
    reset_if_new_minute()

    day_count = int(_get_airflow_var(VAR_DAY_COUNT, 0))
    min_count = int(_get_airflow_var(VAR_MIN_COUNT, 0))

    if day_count >= DAILY_LIMIT:
        raise RuntimeError("[QUOTA] Daily Google NLP quota limit reached")
    if min_count >= MINUTE_LIMIT:
        raise RuntimeError("[QUOTA] Per-minute Google NLP quota limit reached")

    _set_airflow_var(VAR_DAY_COUNT, day_count + 1)
    _set_airflow_var(VAR_MIN_COUNT, min_count + 1)


# Optional: expose for logging/debugging

def get_quota_state():
    return {
        "day": int(_get_airflow_var(VAR_DAY_COUNT, 0)),
        "day_start": _get_airflow_var(VAR_DAY_START, str(datetime.utcnow().date())),
        "minute": int(_get_airflow_var(VAR_MIN_COUNT, 0)),
        "minute_start": float(_get_airflow_var(VAR_MIN_START, time.time())),
    }


def is_homepage(trimmed_page_url: str, site: str) -> bool:
    if not trimmed_page_url or not site:
        return False
    
    trimmed = trimmed_page_url.strip().lower().rstrip("/")
    site_clean = site.strip().lower().replace("www.", "")

    return trimmed == site_clean