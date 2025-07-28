from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
from google.cloud import bigquery
from hashlib import sha256
from datetime import datetime, timezone
import pandas as pd

def hash_url(url):
    return sha256(url.encode("utf-8")).hexdigest()

def check_cache_for_urls(url_hashes, project_id, dataset_id, table_id):
    """
    Query BigQuery to retrieve cached classification results for a list of URL hashes.
    Returns a dict mapping url_hash → row data
    """
    if not url_hashes:
        return {}
    
    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT url_hash, zartico_category, content_topic, prediction_confidence, review_flag, nlp_raw_categories,
               trimmed_page_url, site, page_url, client_id, last_accessed, view_count
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE url_hash IN ({','.join([f"'{h}'" for h in url_hashes])})
    """
    
    results = client.query(query).result()
    cache = {row.url_hash: row for row in results}
    return cache

def update_bq_cache(client, row, project_id, dataset_id, table_id):
    table = f"{project_id}.{dataset_id}.{table_id}"

    url_hash = row["url_hash"]

    merge_query = f"""
    MERGE {table} T
    USING (SELECT '{url_hash}' AS url_hash) S
    ON T.url_hash = S.url_hash
    WHEN MATCHED THEN
      UPDATE SET
        T.last_accessed = CURRENT_TIMESTAMP(),
        T.view_count = IFNULL(T.view_count, 0) + 1
    """

    try:
        client.query(merge_query).result()
        print(f"[DEBUG] Updated cache for {url_hash}")
    except Exception as e:
        print(f"[ERROR] MERGE failed for {url_hash}: {e}")

def get_result_columns():
    return [
        "url_hash", "created_at", "trimmed_page_url", "site", "page_url",
        "content_topic", "prediction_confidence", "review_flag",
        "nlp_raw_categories", "client_id", "last_accessed", "view_count"
    ]

def filter_cache(df):
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

    df["is_cached"] = df["trimmed_page_url"].apply(lambda u: hash_url(u) in cached_results)
    for idx, row in df[df["is_cached"]].iterrows():
        url_hash = hash_url(row["trimmed_page_url"])
        update_bq_cache(client, {
            "url_hash": url_hash,
            # Only view_count and last_accessed are updated in your merge query
        }, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)

    # Filter out cached rows
    uncached_df = df[~df["is_cached"]].copy()
    if uncached_df.empty:
        print("[INFO] All URLs are cached. No uncached URLs to process.")
        return pd.DataFrame(columns=get_result_columns())

    return uncached_df

