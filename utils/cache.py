from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, lit
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
from google.cloud import bigquery
from hashlib import sha256
from datetime import datetime, timezone
import pandas as pd
import uuid

def hash_url(url):
    return sha256(url.encode("utf-8")).hexdigest()

def get_result_columns() -> list[str]:
    return [
        "url_hash", "created_at", "trimmed_page_url", "site", "page_url",
        "content_topic", "prediction_confidence", "review_flag",
        "nlp_raw_categories", "client_id", "last_accessed", "view_count"
    ]

def check_cache_for_urls(client, url_hashes, project_id, dataset_id, table_id):
    """
    Query BigQuery to retrieve cached classification results for a list of URL hashes.
    Returns a dict mapping url_hash → row data
    """
    if not url_hashes:
        return {}

    query = f"""
        SELECT url_hash, zartico_category, content_topic, prediction_confidence, review_flag, nlp_raw_categories,
               trimmed_page_url, site, page_url, client_id, last_accessed, view_count
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE url_hash IN ({','.join([f"'{h}'" for h in url_hashes])})
    """
    
    results = client.query(query).result()
    cache = {row.url_hash: row for row in results}
    return cache

def update_bq_cache_bulk(client, url_hashes, project_id, dataset_id, table_id):
    """Bulk update last_accessed and view_count for cached URLs in BigQuery."""""
    table = f"{project_id}.{dataset_id}.{table_id}"

    url_hash_values = ",\n        ".join([f"('{h}')" for h in url_hashes])
    
    merge_query = f"""
        MERGE `{table}` T
        USING (SELECT url_hash FROM UNNEST([
            {url_hash_values}
        ]) AS url_hash) S
        ON T.url_hash = S.url_hash
        WHEN MATCHED THEN
          UPDATE SET
            T.last_accessed = CURRENT_TIMESTAMP(),
            T.view_count = IFNULL(T.view_count, 0) + 1
    """


    try:
        client.query(merge_query).result()
        print(f"[DEBUG] Bulk updated cache for {len(url_hashes)} cached entries")
    except Exception as e:
        print(f"[ERROR] Bulk MERGE failed for cached entries: {e}")


def filter_cache(df: pd.DataFrame) -> pd.DataFrame:
    """ Filter out URLs that are already cached in BigQuery.
    Updates last_accessed and view_count for cached entries.
    Returns a DataFrame with uncached URLs ready for processing.
    """
    client = bigquery.Client(project=PROJECT_ID)

    # Edge cases
    if df.empty or "trimmed_page_url" not in df.columns:
        print("[FILTER] Input DataFrame is empty or missing 'trimmed_page_url'")
        return pd.DataFrame(columns=get_result_columns())

    # Compute hashes once and add to dataframe
    df["url_hash"] = df["trimmed_page_url"].apply(hash_url)

    all_hashes = df["url_hash"].tolist()
    if not all_hashes:
        print("[FILTER] No URL hashes to check.")
        return pd.DataFrame(columns=get_result_columns())

    cached_results = check_cache_for_urls(client, all_hashes, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)
    df["is_cached"] = df["url_hash"].apply(lambda h: h in cached_results)

    # Bulk update access metadata for cached entries
    cached_hashes = df[df["is_cached"]]["url_hash"].tolist()
    update_bq_cache_bulk(client, cached_hashes, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)

    # Filter out cached rows
    uncached_df = df[~df["is_cached"]].copy()
    if uncached_df.empty:
        print("[INFO] All URLs are cached. No uncached URLs to process.")
        return pd.DataFrame(columns=get_result_columns())
    
    # Remove temporary columns that shouldn't be in the final output
    uncached_df = uncached_df.drop(columns=["is_cached"], errors="ignore")

    print(f"[FILTER] {len(cached_hashes)} URLs were cached. {len(uncached_df)} URLs remain to process.")
    return uncached_df

# -----------------------
# BigQuery MERGE helpers
# -----------------------

def _touches_table_name(run_id: str | None = None) -> str:
    """
    Fully-qualified temp table name for cache touches. We drop it after MERGE.
    """
    suffix = (run_id or uuid.uuid4().hex).replace("-", "_")
    return f"{PROJECT_ID}.{BQ_DATASET_ID}.tmp_cached_touches_{suffix}"

def _merge_touches_with_counts(client: bigquery.Client, touches_table: str):
    """
    touches_table schema: (url_hash STRING, dup_hits INT64)
    Increments view_count by dup_hits for existing rows and sets last_accessed.
    Drops the temp table when done to keep the dataset clean.
    """
    merge_sql = f"""
    MERGE `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}` T
    USING `{touches_table}` S
    ON T.url_hash = S.url_hash
    WHEN MATCHED THEN
      UPDATE SET
        T.last_accessed = CURRENT_TIMESTAMP()
    """
    client.query(merge_sql).result()

    # Drop temp table to avoid clutter
    client.query(f"DROP TABLE `{touches_table}`").result()
    print(f"[FILTER] Touch MERGE complete and dropped {touches_table}")


def filter_cache_spark(
    spark,
    df: DataFrame,
    *,
    run_id: str | None = None,
    include_staging_check: bool = True,
    staging_table: str = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_info",
    temp_gcs_bucket: str | None = None
) -> DataFrame:
    """
    Preserves access frequency and prevents NLP duplicates:

    1) Require url_hash (computed in transform).
    2) dup_hits = count(*) per url_hash in this slice.
    3) For hashes already in cache: write (url_hash, dup_hits) to a temp BQ table and MERGE to bump
       view_count += dup_hits and set last_accessed = now. (No driver collect, race-safe.)
    4) For uncached hashes: return ONE canonical row per url_hash for NLP, carrying 'access_hits = dup_hits'.

    Returns: Spark DF with columns from the input (+ access_hits), one row per uncached url_hash.
    """
    # Require url_hash
    if "url_hash" not in df.columns:
        raise ValueError("[FILTER] Expected 'url_hash' to exist. Compute it in transform first.")

    # 2) Count duplicates in this slice
    dup_counts = df.groupBy("url_hash").agg(F.count(F.lit(1)).alias("dup_hits"))

    # 3) Load cache keys
    cached_keys = (
        spark.read.format("bigquery")
        .option("table", f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}")
        .load()
        .select("url_hash", "content_topic")
        .where(F.col("content_topic").isNotNull())
        .select("url_hash")
        .distinct()
    )

    # Split cached vs. uncached (still at slice granularity)
    df_with_counts = df.join(dup_counts, on="url_hash", how="left")
    df_cached   = df_with_counts.join(cached_keys, on="url_hash", how="left_semi")
    df_uncached = df_with_counts.join(cached_keys, on="url_hash", how="left_anti")

    # Touch MERGE for cached (no collect): write -> MERGE -> drop
    cached_touches = df_cached.select("url_hash", "dup_hits").distinct()
    cached_count = cached_touches.count()
    if cached_count > 0:
        touches_table = _touches_table_name(run_id)
        (cached_touches.write
            .format("bigquery")
            .option("table", touches_table)
            .option("temporaryGcsBucket", temp_gcs_bucket or "")
            .mode("overwrite")
            .save())
        client = bigquery.Client(project=PROJECT_ID)
        _merge_touches_with_counts(client, touches_table)

    # Uncached
    rep = (
        df_uncached
        .withColumn("url_len", F.length(col("page_url")))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("url_hash").orderBy(col("url_len").asc(), col("page_url").asc())
            )
        )
        .filter(col("rn") == 1)
        .drop("rn", "url_len")
    )

    remaining = rep.count()
    print(f"[FILTER] Cached touched: {cached_count} url_hashes. Uncached to process: {remaining}")
    return rep
