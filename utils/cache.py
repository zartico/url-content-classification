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
    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT url_hash, content_topic, prediction_confidence, review_flag, nlp_raw_categories,
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