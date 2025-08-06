from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
from google.cloud import bigquery
from hashlib import sha256
from datetime import datetime, timezone
import pandas as pd

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
    uncached_df = uncached_df.drop(columns=["url_hash", "is_cached"], errors="ignore")

    print(f"[FILTER] {len(cached_hashes)} URLs were cached. {len(uncached_df)} URLs remain to process.")
    return uncached_df

def filter_cache_spark(spark, df: DataFrame) -> DataFrame:
    """
    Spark version of filter_cache().
    Filters out cached URLs by joining with the BigQuery cache table on url_hash.
    Updates view_count and last_accessed for cached entries.
    Returns a Spark DataFrame with only uncached URLs.
    """
    if "trimmed_page_url" not in df.columns:
        print("[FILTER] Input DataFrame missing 'trimmed_page_url'")
        return spark.createDataFrame([], schema=get_result_columns())

    # Step 1: UDF to hash trimmed_page_url
    hash_udf = udf(lambda url: hash_url(url), StringType())
    df = df.withColumn("url_hash", hash_udf(col("trimmed_page_url")))

    # Step 2: Load cache table from BigQuery
    cached_df = spark.read \
        .format("bigquery") \
        .option("table", f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}") \
        .load() \
        .select("url_hash")

    # Step 3: Identify cached rows using join
    df_joined = df.join(cached_df, on="url_hash", how="left_outer")
    df_with_flag = df_joined.withColumn("is_cached", col("cached_df.url_hash").isNotNull())
    
    # Step 4: Extract cached hashes and update metadata (view_count + last_accessed)
    cached_hashes = (
        df_with_flag.filter(col("is_cached"))
        .select("url_hash")
        .distinct()
        .rdd.flatMap(lambda row: row)
        .collect()
    )

    if cached_hashes:
        client = bigquery.Client(project=PROJECT_ID)
        update_bq_cache_bulk(client, cached_hashes, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)

    # Step 5: Filter out cached rows
    uncached_df = df_with_flag.filter(~col("is_cached")).drop("url_hash", "is_cached")

    remaining = uncached_df.count()
    print(f"[FILTER] {len(cached_hashes)} URLs were cached. {remaining} URLs remain to process.")
    return uncached_df