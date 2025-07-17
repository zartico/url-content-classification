from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
from utils.cache import hash_url, check_cache_for_urls, update_bq_cache
from utils.web_fetch import fetch_all_pages
from google.cloud import language_v1, bigquery
from datetime import datetime, timezone
import pandas as pd
import asyncio
import time


def classify_text(text):
    """ Classify text using Google Cloud Natural Language API."""

    try:
        client = language_v1.LanguageServiceClient(
            client_options={"quota_project_id": PROJECT_ID}
        )
        document = language_v1.Document(
            content=text,
            type_=language_v1.Document.Type.PLAIN_TEXT
        )
        response = client.classify_text(
            request={
                "document": document,
                "classification_model_options": {"v2_model": {}}
            }
        )
        return response.categories
    except Exception as e:
        print(f"[ERROR] NLP classification failed: {e}")
        return []
    
# Main categorization function
def categorize_urls(df):
    client = bigquery.Client(project=PROJECT_ID)
    assert "trimmed_page_url" in df.columns
    assert "page_url" in df.columns

    trimmed_urls = df["trimmed_page_url"].tolist() # For hashing
    url_hashes = [hash_url(url) for url in trimmed_urls]
    # Check existing cache in bulk
    cached_results = check_cache_for_urls(url_hashes, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)

    page_urls = df["page_url"].tolist() # For fetching text, classification
    page_text_map = asyncio.run(fetch_all_pages(page_urls))

    # Columns to be added to the DataFrame
    content_topics, confidences, review_flags, raw_categories = [], [], [], []
    created_ats, last_accesseds, view_counts= [], [], []
    
    # Track cached indexes
    cached_indexes = []

    for idx, (trimmed_url, page_url) in enumerate(zip(df["trimmed_page_url"], df["page_url"])):
        url_hash = hash_url(trimmed_url)
        now_str = datetime.now(timezone.utc).isoformat()

        cached = cached_results.get(url_hash)
        # If cached result exists, use it
        if cached is not None:
            # Update last accessed time and view count in cache
            update_bq_cache(client, {
                "url_hash": url_hash,
                "content_topic": cached.content_topic,
                "prediction_confidence": cached.prediction_confidence,
                "review_flag": cached.review_flag,
                "nlp_raw_categories": cached.nlp_raw_categories,
                "trimmed_page_url": trimmed_url,
                "site": getattr(cached, "site", None),
                "page_url": page_url,
                "client_id": getattr(cached, "client_id", None),
                "view_count": getattr(cached, "view_count", 0)
            }, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)

            # Mark for removal from df
            cached_indexes.append(idx)
            continue

        created_ats.append(now_str)
        last_accesseds.append(now_str)
        
        # If not cached, fetch the page text
        text = page_text_map.get(page_url, "")
        if len(text.split()) < 20: # Too short to classify
            content_topics.append(None)
            confidences.append(None)
            review_flags.append(True)
            raw_categories.append(None)
            view_counts.append(1)
            continue

        try: # Classify the text using NLP
            categories = classify_text(text)
            print(f"[DEBUG] Categories returned for {page_url}: {categories}")
            if categories:
                top_cat = categories[0]
                content_topics.append(top_cat.name)
                confidences.append(top_cat.confidence)
                review_flags.append(top_cat.confidence < 0.6)
                raw_categories.append(str([{"name": c.name, "confidence": c.confidence} for c in categories]))
            else: # No categories found
                content_topics.append(None)
                confidences.append(None)
                review_flags.append(True)
                raw_categories.append(None)

            view_counts.append(1)

        except Exception as e:
            print(f"[ERROR] NLP failed for {page_url}: {e}")
            content_topics.append(None)
            confidences.append(None)
            review_flags.append(True)
            raw_categories.append(None)
            view_counts.append(1)

        time.sleep(0.5)  # Rate limit

    # Remove cached rows from df
    df = df.drop(index=cached_indexes).reset_index(drop=True)

    # If no new rows remain, return empty DataFrame with correct columns
    if len(df) == 0:
        # List all columns you expect downstream
        return pd.DataFrame(columns=[
            "url_hash", "created_at", "trimmed_page_url", "site", "page_url",
            "content_topic", "prediction_confidence", "review_flag",
            "nlp_raw_categories", "client_id", "last_accessed", "view_count"
        ])

    # Create a Pandas DataFrame with the new URL results
    df["url_hash"] = url_hashes
    df["created_at"] = created_ats
    df["content_topic"] = content_topics
    df["prediction_confidence"] = confidences
    df["review_flag"] = review_flags
    df["nlp_raw_categories"] = raw_categories
    df["last_accessed"] = last_accesseds
    df["view_count"] = view_counts
    assert all(len(lst) == len(df) for lst in [url_hashes, content_topics, confidences, review_flags, raw_categories, view_counts]), "Length mismatch in output columns"
    return df


