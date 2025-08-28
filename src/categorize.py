from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
from utils.cache import hash_url, get_result_columns
from utils.category_mapping import map_to_zartico_category
from utils.utils import is_homepage

from google.cloud import language_v1, bigquery
from google.api_core.retry import Retry, if_exception_type
from google.api_core import exceptions

from bs4 import BeautifulSoup
from datetime import datetime, timezone
import pandas as pd
import random, time
from time import monotonic as _monotonic


#----- Tunables for Throttling/Retries -----

RPS_PER_TASK = 5.0                # ~5 requests/second per categorize task
JITTER_FRACTION = 0.10            # +/-10% jitter on pacing sleeps
POST_CALL_JITTER_MAX_SEC = 0.05   # up to 50ms small jitter after each call

RETRY = Retry(
    predicate=if_exception_type(
        exceptions.ResourceExhausted,    # 429 / quota
        exceptions.ServiceUnavailable,   # 503
        exceptions.DeadlineExceeded,     # server-side timeout
    ),
    initial=1.0, 
    maximum=10.0, 
    multiplier=1.5, 
    deadline=60.0,
)

class _RateLimiter:
    """
    Simple tokenless rate limiter: enforces ~RPS by spacing calls at ~interval seconds.
    Adds small jitter so many tasks don't align on the same boundaries.
    """
    def __init__(self, rps: float, jitter_fraction: float = 0.10):
        if rps <= 0:
            raise ValueError("rps must be > 0")
        self.interval = 1.0 / rps
        self.jitter_fraction = max(0.0, jitter_fraction)
        self._next = _monotonic()

    def acquire(self):
        now = _monotonic()
        # If we're early, sleep the remaining time (+/- jitter)
        if now < self._next:
            remaining = self._next - now
            # jitter in [-j, +j] where j = remaining * jitter_fraction
            j = remaining * self.jitter_fraction
            sleep_for = max(0.0, remaining + random.uniform(-j, j))
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = _monotonic()
        # schedule next slot
        self._next = max(self._next, now) + self.interval


def classify_text(raw_html, client):
    """ Classify text using Google Cloud Natural Language API."""

    try:
        # Extract and sanitize visible text
        soup = BeautifulSoup(raw_html, "html.parser")
        text = soup.get_text(separator=' ', strip=True)

        # tiny jitter to avoid lockstep bursts across mapped tasks
        time.sleep(random.uniform(0, POST_CALL_JITTER_MAX_SEC))

        # Call Google NLP V2 API
        document = language_v1.Document(
            content=text,
            type_=language_v1.Document.Type.PLAIN_TEXT
        )
        response = client.classify_text(
            request={
                "document": document,
                "classification_model_options": {"v2_model": {}}
            }, 
            retry = RETRY,
            timeout=30,
        )
        return response.categories
    except Exception as e:
        print(f"[ERROR] NLP classification failed: {e}")
        return None
    

#----- Main Categorization Function -----

def categorize_urls(df):
    client = bigquery.Client(project=PROJECT_ID)

    nlp_client = language_v1.LanguageServiceClient(
        client_options={"quota_project_id": PROJECT_ID}
    )

    # Edge case
    if df.empty:
        print("[DEBUG] Empty DataFrame received, returning empty result.")
        return pd.DataFrame(columns=get_result_columns())
    
    if "page_text" not in df.columns:
        print("[DEBUG] Missing page_text, returning empty result.")
        return pd.DataFrame(columns=get_result_columns())
    
    # Per-task limiter
    limiter = _RateLimiter(RPS_PER_TASK, JITTER_FRACTION)

    # Rows to be added to the DataFrame
    rows = []

    for idx, row in df.iterrows():
        page_url = row["page_url"]
        trimmed_url = row["trimmed_page_url"]
        site = row["site"]
        page_text = row.get("page_text", "")

        # Skip if not enough text for NLP
        if not page_text or len(str(page_text).split()) < 20:
            print(f"[SKIP] {page_url} insufficient content.")
            continue

        # url_hash: prefer provided, else compute (back-compat)
        url_hash = row.get("url_hash")
        if pd.isna(url_hash) or url_hash is None:
            url_hash = hash_url(trimmed_url)

        # Respect per-task RPS before the API call
        limiter.acquire()

        categories = classify_text(page_text, nlp_client)
        if categories is None:
            # Hard failure (exception) → skip so it can be retried in future runs
            print(f"[SKIP] {page_url} NLP error; skipping insert.")
            continue
        if not categories:
            # No categories returned → treat as unsuccessful; skip so it retries later
            print(f"[SKIP] {page_url} no categories; skipping insert.")
            continue

        # Top category + mapping
        top_cat = categories[0]
        if is_homepage(trimmed_url, site):
            zartico_cat = "Navigation & Home Page"
        else:
            zartico_cat = map_to_zartico_category(top_cat.name)

        # Seed view_count with access_hits if present; else 1
        try:
            vc = int(row.get("access_hits", 1))
        except Exception:
            vc = 1
        vc = max(1, vc)

        now_ts = datetime.now(timezone.utc).isoformat()

        rows.append({
            "url_hash": url_hash,
            "created_at": now_ts,
            "trimmed_page_url": trimmed_url,
            "site": site,
            "page_url": page_url,
            "zartico_category": zartico_cat,
            "content_topic": top_cat.name,
            "prediction_confidence": top_cat.confidence,
            "review_flag": top_cat.confidence < 0.6,
            "nlp_raw_categories": str([{"name": c.name, "confidence": c.confidence} for c in categories]),
            "client_id": row["client_id"] if "client_id" in df.columns else None,
            "last_accessed": now_ts,
            "view_count": vc,
        })

        # small post-call jitter
        if POST_CALL_JITTER_MAX_SEC > 0:
            time.sleep(random.uniform(0, POST_CALL_JITTER_MAX_SEC))

    if not rows:
        print("[DEBUG] No successful categorizations this batch.")
        return pd.DataFrame(columns=get_result_columns())

    result_df = pd.DataFrame(rows, columns=get_result_columns())

    # Debug summary
    print("[DEBUG] Categorization summary:")
    print("  total_input_rows:", len(df))
    print("  successful_rows:", len(result_df))
    print("  url_hashes:", result_df["url_hash"].nunique())
    print("  content_topics:", result_df["content_topic"].notna().sum())
    print("  zartico_categories:", result_df["zartico_category"].notna().sum())
    print("  confidences_nonnull:", result_df["prediction_confidence"].notna().sum())
    print("  avg_confidence:", result_df["prediction_confidence"].mean())
    print("  review_flags_true:", result_df["review_flag"].sum())
    print("  view_count_total:", result_df["view_count"].sum())

    return result_df

