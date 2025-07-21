from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
from utils.cache import hash_url, check_cache_for_urls, update_bq_cache, get_result_columns
from utils.web_fetch import fetch_all_pages
from utils.category_mapping import map_to_zartico_category
from utils.utils import is_homepage
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

    # Edge cases
    if df.empty or "trimmed_page_url" not in df.columns:
        return pd.DataFrame(columns=get_result_columns())

    trimmed_urls = df["trimmed_page_url"].tolist() # For hashing
    if not trimmed_urls:
        return pd.DataFrame(columns=get_result_columns())
    
    url_hashes_all = [hash_url(url) for url in trimmed_urls]
    if not url_hashes_all:
        return pd.DataFrame(columns=get_result_columns())
    
    # Check existing cache in bulk
    cached_results = check_cache_for_urls(url_hashes_all, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)

    page_urls = df["page_url"].tolist() # For fetching text, classification
    page_text_map = asyncio.run(fetch_all_pages(page_urls))

    # Columns to be added to the DataFrame
    zartico_categories, content_topics, confidences, review_flags, raw_categories = [], [], [], [], []
    url_hashes, created_ats, last_accesseds, view_counts= [], [], [], []
    
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
                "zartico_category": cached.zartico_category,
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
        
        url_hashes.append(url_hash)
        created_ats.append(now_str)
        last_accesseds.append(now_str)
        
        # If not cached, fetch the page text
        text = page_text_map.get(page_url, "")
        if len(text.split()) < 20: # Too short to classify
            zartico_categories.append(None)
            content_topics.append(None)
            confidences.append(None)
            review_flags.append(True)
            raw_categories.append(None)
            view_counts.append(1)
            continue

        try: # Classify the text using NLP
            # Check quota before proceeding 
            # check_and_increment_quota() ** UNCOMMENT THIS LINE IN PRODUCTION **
            categories = classify_text(text)
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

        time.sleep(0.5)  # Rate limit

    # Remove cached rows from df
    df = df.drop(index=cached_indexes).reset_index(drop=True)

    # If no new rows remain, return empty DataFrame with correct columns
    if len(df) == 0:
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
    df["url_hash"] = url_hashes
    df["created_at"] = created_ats
    df["zartico_category"] = zartico_categories
    df["content_topic"] = content_topics
    df["prediction_confidence"] = confidences
    df["review_flag"] = review_flags
    df["nlp_raw_categories"] = raw_categories
    df["last_accessed"] = last_accesseds
    df["view_count"] = view_counts

    return df


