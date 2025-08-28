from pyspark.sql import DataFrame
from pyspark.sql.types import *
from google.cloud import bigquery
from google.cloud.bigquery import ArrayQueryParameter
from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID, GA4_DATASET_ID, GA4_TABLE_ID
from google.api_core.exceptions import NotFound

import pandas as pd
import os

STAGING_INCOMING = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_categorized_incoming"
FINAL_TABLE      = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

#---- Schema and Table Management ----

def ensure_dataset_and_table_exist():
    """
    Ensure the BigQuery dataset and table exist, creating them if necessary. 
    """

    client = bigquery.Client(project = PROJECT_ID)
    # Define dataset and table references
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, BQ_DATASET_ID)
    table_ref = dataset_ref.table(BQ_TABLE_ID)

    # Check/create dataset
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        print(f"[ERROR] Dataset {BQ_DATASET_ID} does not exist. Please ask your admin to create it.")
        return None 

    # Check/create table
    try:
        client.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField("url_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", default_value_expression="CURRENT_TIMESTAMP()"),
            bigquery.SchemaField("trimmed_page_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("site", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("page_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zartico_category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("content_topic", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("prediction_confidence", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("review_flag", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("nlp_raw_categories", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("client_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_accessed", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("view_count", "INTEGER", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"[INFO] Created table {BQ_TABLE_ID}.")
    
    return table_ref

def _ensure_staging_exists(client: bigquery.Client, schema: list[bigquery.SchemaField]):
    """
    Create the fixed staging table if it doesn't exist.
    We'll WRITE_TRUNCATE into it for each batch and DROP it after the MERGE.
    """
    try:
        client.get_table(STAGING_INCOMING)
    except NotFound:
        client.create_table(bigquery.Table(STAGING_INCOMING, schema=schema))
        print(f"[INFO] Created staging table {STAGING_INCOMING}.")

#---- Reconciliation of View Counts ----

def _iter_chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _reconcile_lifetime_view_counts_for_hashes(client: bigquery.Client, hashes: list[str], chunk_size: int = 5000):
    """
    Idempotent: for the provided url_hashes only, set FINAL.view_count to lifetime GA4 hits.
    Meant to be called from load_data() when skip_reconcile=False.
    """
    if not hashes:
        return
    total = 0
    for chunk in _iter_chunks(hashes, chunk_size):
        # 1. Write the CTE result to a temp table
        temp_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.tmp_lifetime_hits"
        sql_cte = f"""
        CREATE OR REPLACE TABLE `{temp_table}` AS
        SELECT
            TO_HEX(SHA256(CAST(LOWER(TRIM(trimmed_page_url)) AS STRING))) AS url_hash,
            COUNT(*) AS lifetime_hits
        FROM `{PROJECT_ID}.{GA4_DATASET_ID}.{GA4_TABLE_ID}`
        WHERE trimmed_page_url IS NOT NULL
            AND site IS NOT NULL
            AND client_id NOT LIKE '%Demo%'
            AND TO_HEX(SHA256(CAST(LOWER(TRIM(trimmed_page_url)) AS STRING))) IN UNNEST(@hashes)
        GROUP BY url_hash
        """
        client.query(
            sql_cte,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[ArrayQueryParameter("hashes", "STRING", chunk)]
            ),
        ).result()

        # 2. Run MERGE using the temp table
        merge_sql = f"""
        MERGE `{FINAL_TABLE}` T
        USING `{temp_table}` L
        ON T.url_hash = L.url_hash
        WHEN MATCHED THEN UPDATE SET
          T.view_count    = L.lifetime_hits,
          T.last_accessed = CURRENT_TIMESTAMP()
        """
        client.query(merge_sql).result()

        # 3. Drop temp table
        client.delete_table(temp_table, not_found_ok=True)
        total += len(chunk)
    print(f"[LOAD] Lifetime view_count reconciled for {total} urls.")

#---- Main Load Function ----

def load_data(df: pd.DataFrame, *, skip_reconcile: bool = False):
    """
    Simple, clean loader:
      1) Ensure final table exists.
      2) Filter to successful rows (content_topic + prediction_confidence).
      3) Overwrite a fixed staging table with this batch.
      4) MERGE into the final table:
         - MATCHED: update topic/confidence if better; set last_accessed; DO NOT change view_count.
         - NOT MATCHED: insert new row with view_count from payload (seeded from access_hits).
      5) DROP the staging table to keep the dataset tidy.
    """
    table_ref = ensure_dataset_and_table_exist()
    if table_ref is None or df is None or df.empty:
        print("[LOAD] No data loaded (empty DataFrame or table missing).")
        return

    # Keep only successful categorizations
    mask = df["content_topic"].notna() & df["prediction_confidence"].notna()
    df_ok = df.loc[mask].copy()
    if df_ok.empty:
        print("[LOAD] No successful rows to load after filtering.")
        return

    # Enforce column order the MERGE expects
    cols = [
        "url_hash", "created_at", "trimmed_page_url", "site", "page_url",
        "zartico_category", "content_topic", "prediction_confidence",
        "review_flag", "nlp_raw_categories", "client_id",
        "last_accessed", "view_count",
    ]
    df_ok = df_ok[cols]

    client = bigquery.Client(project=PROJECT_ID)

    # Staging schema mirrors final (subset/ordering is fine)
    staging_schema = [
        bigquery.SchemaField("url_hash", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("trimmed_page_url", "STRING"),
        bigquery.SchemaField("site", "STRING"),
        bigquery.SchemaField("page_url", "STRING"),
        bigquery.SchemaField("zartico_category", "STRING"),
        bigquery.SchemaField("content_topic", "STRING"),
        bigquery.SchemaField("prediction_confidence", "FLOAT64"),
        bigquery.SchemaField("review_flag", "BOOLEAN"),
        bigquery.SchemaField("nlp_raw_categories", "STRING"),
        bigquery.SchemaField("client_id", "STRING"),
        bigquery.SchemaField("last_accessed", "TIMESTAMP"),
        bigquery.SchemaField("view_count", "INT64"),
    ]
    _ensure_staging_exists(client, staging_schema)

    # Stage only if we actually have rows
    if len(df_ok) == 0:
        print("[LOAD] Nothing to stage.")
        return

    # 1) Stage with WRITE_TRUNCATE
    client.load_table_from_dataframe(
        df_ok,
        STAGING_INCOMING,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()
    print(f"[LOAD] Staged {len(df_ok)} rows to {STAGING_INCOMING} (WRITE_TRUNCATE)")

    # 2) MERGE → final (no view_count change on updates)
    merge_sql = f"""
    MERGE `{FINAL_TABLE}` T
    USING `{STAGING_INCOMING}` S
    ON T.url_hash = S.url_hash
    WHEN MATCHED THEN
      UPDATE SET
        T.content_topic         = IF(S.prediction_confidence > T.prediction_confidence, S.content_topic, T.content_topic),
        T.prediction_confidence = GREATEST(IFNULL(T.prediction_confidence, 0), IFNULL(S.prediction_confidence, 0)),
        T.review_flag           = IF(S.prediction_confidence > T.prediction_confidence, S.review_flag, T.review_flag),
        T.nlp_raw_categories    = IF(S.prediction_confidence > T.prediction_confidence, S.nlp_raw_categories, T.nlp_raw_categories),
        T.zartico_category      = IF(S.prediction_confidence > T.prediction_confidence, S.zartico_category, T.zartico_category),
        T.last_accessed         = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (url_hash, created_at, trimmed_page_url, site, page_url,
              zartico_category, content_topic, prediction_confidence, review_flag,
              nlp_raw_categories, client_id, last_accessed, view_count)
      VALUES (S.url_hash, S.created_at, S.trimmed_page_url, S.site, S.page_url,
              S.zartico_category, S.content_topic, S.prediction_confidence, S.review_flag,
              S.nlp_raw_categories, S.client_id, CURRENT_TIMESTAMP(), S.view_count)
    """
    client.query(merge_sql).result()
    print(f"[LOAD] MERGE -> {FINAL_TABLE} complete.")

    # 3) Optional reconcile: set FINAL.view_count to *lifetime* GA4 hits for this batch
    hashes = df_ok["url_hash"].dropna().unique().tolist()
    if skip_reconcile:
        print("[LOAD] Skipping lifetime reconcile (skip_reconcile=True).")
    else:
        _reconcile_lifetime_view_counts_for_hashes(client, hashes)

    # 4) Drop the staging table to keep the dataset clean
    try:
        client.delete_table(STAGING_INCOMING, not_found_ok=True)
        print(f"[LOAD] Dropped staging table {STAGING_INCOMING}")
    except Exception as e:
        # Non-fatal; table will be overwritten next run anyway
        print(f"[WARN] Failed to drop staging table {STAGING_INCOMING}: {e}")


