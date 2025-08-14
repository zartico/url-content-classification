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

def classify_text(raw_html, client):
    """ Classify text using Google Cloud Natural Language API."""

    try:
        # Extract and sanitize visible text
        soup = BeautifulSoup(raw_html, "html.parser")
        text = soup.get_text(separator=' ', strip=True)

        # tiny jitter to avoid lockstep bursts across mapped tasks
        time.sleep(random.uniform(0, 0.2))

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
        return []
    

# Main categorization function
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
    
    # Columns to be added to the DataFrame
    zartico_categories, content_topics, confidences, review_flags, raw_categories = [], [], [], [], []
    url_hashes, created_ats, last_accesseds, view_counts= [], [], [], []
    processed_indexes = []

    for idx, row in df.iterrows():
        page_url = row["page_url"]
        trimmed_url = row["trimmed_page_url"]
        site = row["site"]
        page_text = row.get("page_text", "")

        # Use existing url_hash if available, otherwise create it (for backward compatibility)
        if "url_hash" in df.columns and pd.notna(row.get("url_hash")):
            url_hash = row["url_hash"]
        else:
            url_hash = hash_url(trimmed_url)
        
        now_ts = datetime.now(timezone.utc).isoformat()

        # If no text or not long enough for NLP API, skip this URL
        if not page_text or len(page_text.split()) < 20:
            print(f"[SKIP] Skipping {page_url} due to missing or insufficient content.")
            continue  # Skip this row entirely

        processed_indexes.append(idx)
        url_hashes.append(url_hash)
        created_ats.append(now_ts)
        last_accesseds.append(now_ts)
        
        try: # Classify the text using NLP
            categories = classify_text(page_text, nlp_client)
            print(f"[DEBUG] Categories returned for {page_url}: {categories}")
            if categories:
                top_cat = categories[0]

                # Site is a homepage
                if is_homepage(trimmed_url, df["site"][idx]):
                    zartico_categories.append("Navigation & Home Page")
                else:
                    zartico_categories.append(map_to_zartico_category(top_cat.name))

                content_topics.append(top_cat.name)
                confidences.append(top_cat.confidence)
                review_flags.append(top_cat.confidence < 0.6)
                raw_categories.append(str([{"name": c.name, "confidence": c.confidence} for c in categories]))

            else: # No categories found
                zartico_categories.append(None)
                content_topics.append(None)
                confidences.append(None)
                review_flags.append(True)
                raw_categories.append(None)

            view_counts.append(1)

        except Exception as e:
            print(f"[ERROR] NLP failed for {page_url}: {e}")
            zartico_categories.append(None)
            content_topics.append(None)
            confidences.append(None)
            review_flags.append(True)
            raw_categories.append(None)
            view_counts.append(1)

    # If no new rows remain, return empty DataFrame with correct columns
    if not processed_indexes or len(df) == 0:
        return pd.DataFrame(columns=get_result_columns())

    # Debug
    print("[DEBUG] DataFrame length:", len(df))
    print("[DEBUG] url_hashes:", len(url_hashes))
    print("[DEBUG] created_ats:", len(created_ats))
    print("[DEBUG] zartico_categories:", len(zartico_categories))
    print("[DEBUG] content_topics:", len(content_topics))
    print("[DEBUG] confidences:", len(confidences))
    print("[DEBUG] review_flags:", len(review_flags))
    print("[DEBUG] raw_categories:", len(raw_categories))
    print("[DEBUG] last_accesseds:", len(last_accesseds))
    print("[DEBUG] view_counts:", len(view_counts))


    # Create a Pandas DataFrame with the new URL results
    result_df = pd.DataFrame({
        "url_hash": url_hashes,
        "created_at": created_ats,
        "trimmed_page_url": [df["trimmed_page_url"].iloc[i] for i in processed_indexes],
        "site": [df["site"].iloc[i] for i in processed_indexes],
        "page_url": [df["page_url"].iloc[i] for i in processed_indexes],
        "content_topic": content_topics,
        "prediction_confidence": confidences,
        "review_flag": review_flags,
        "nlp_raw_categories": raw_categories,
        "client_id": [df["client_id"].iloc[i] for i in processed_indexes] if "client_id" in df.columns else [None]*len(processed_indexes),
        "last_accessed": last_accesseds,
        "view_count": view_counts,
        "zartico_category": zartico_categories,
    })

    return result_df

