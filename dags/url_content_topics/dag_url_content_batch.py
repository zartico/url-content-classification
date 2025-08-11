from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable


from pyspark.sql.functions import monotonically_increasing_id, floor, col, lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

from google.cloud import bigquery
from datetime import timedelta
import pandas as pd
import asyncio
import uuid
import math
import sys
import os

sys.path.append('/home/airflow/gcs/data/url_content_topics')
sys.path.append('/home/airflow/gcs/data/url_content_topics/src')
sys.path.append('/home/airflow/gcs/data/url_content_topics/utils')
sys.path.append('/home/airflow/gcs/data/url_content_topics/config')

from config.project_config import PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID, BQ_TABLE_ID, BQ_DATASET_ID
from src.load import load_data
from src.extract import extract_data
from src.transform import transform_data
from utils.cache import filter_cache, filter_cache_spark
from utils.web_fetch import fetch_all_pages, extract_visible_text
from src.categorize import categorize_urls
from src.load import load_data

BATCH_SIZE = 50
TOTAL_URLS = 200
MAX_DYNAMIC_TASKS = 500

# Use the GCS bucket configured in Airflow Variables
TEMP_BUCKET = Variable.get("TEMP_GCS_BUCKET")  # e.g. non-codecs-prod-airflow/data/tmp/spark_staging

def create_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("BigQueryIntegration")
        # Pin Spark to local mode and 2 cores so the Airflow worker keeps breathing
        .master("local[2]")
        # Use only the JARs you vendored into the image / GCS-mounted path
        .config("spark.jars", ",".join([
            "/home/airflow/gcs/data/url_content_topics/src/spark_jars/gcs-connector-hadoop3-2.2.21.jar",
            "/home/airflow/gcs/data/url_content_topics/src/spark_jars/spark-3.5-bigquery-0.37.0.jar",
        ]))
        # GCS filesystem hooks
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        # Point BigQuery connector at your temp bucket (writer can still override per-write)
        .config("spark.bigquery.temp.gcs.bucket", TEMP_BUCKET)
        # Conservative memory; your worker is 8GB total
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        # Sensible defaults
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.shuffle.partitions", "8")   # 4 * cores (2)
        .getOrCreate()
    )
    return spark

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["url-content"],
)
def url_content_backfill():
    @task(task_id="extract_data", retries=0, retry_delay=timedelta(minutes=0))
    def extract():
        # Spark version for backfill
        print("[EXTRACT] Starting data extraction from BigQuery")
        spark = create_spark_session()
        raw_df = extract_data(spark, PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID, TOTAL_URLS)
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/raw.parquet"
        raw_df.write.mode("overwrite").parquet(temp_path)
        print(f"[EXTRACT] Extracted {raw_df.count()} rows from BigQuery")
        spark.stop()
        return temp_path
        # Pandas version (without spark)
        # raw_df = extract_data(PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID, TOTAL_URLS)
        # print(f"[EXTRACT] Extracted {len(raw_df)} rows from BigQuery")
        # return raw_df.to_dict(orient="records")

    @task(task_id="transform_data", retries=0, retry_delay=timedelta(minutes=0))
    def transform(raw_path):
        # Spark version for backfill
        print("[TRANSFORM] Starting data transformation")
        spark = create_spark_session()
        raw_df = spark.read.parquet(raw_path)
        print(f"[TRANSFORM] Raw DataFrame loaded with {raw_df.count()} rows")
        transformed_df = transform_data(raw_df)
        print(f"[TRANSFORM] Transformed/Cleaned {transformed_df.count()} rows")
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/transformed.parquet"
        transformed_df.write.mode("overwrite").parquet(temp_path)
        spark.stop()
        return temp_path
        # Pandas version (without spark)
        # raw_df = pd.DataFrame(raw_path)
        # transformed_df = transform_data(raw_df)
        # print(f"[TRANSFORM] Transformed/Cleaned {len(transformed_df)} rows")
        # return transformed_df.to_dict(orient="records")

    @task(task_id="filter_urls", retries=0, retry_delay=timedelta(minutes=0))
    def filter_cached_urls(transformed_path):
        # Spark version for backfill
        print("[FILTER] Start filtering cached URLs")
        spark = create_spark_session()
        df = spark.read.parquet(transformed_path)
        print(f"[FILTER] Loaded from transformed data with {df.count()} rows")
        uncached_df = filter_cache_spark(spark, df)
        print(f"[FILTER] Filtered cached URLs, remaining {uncached_df.count()} uncached rows")
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/uncached.parquet"
        uncached_df.write.mode("overwrite").parquet(temp_path)
        spark.stop()
        return temp_path
        # Pandas version (without spark)
        # df = pd.DataFrame(transformed_path)
        # print(f"[FILTER] Loaded from transformed data with {len(df)} rows")
        # uncached_df = filter_cache(df)
        # print(f"[FILTER] Filtered cached URLs, remaining {len(uncached_df)} uncached rows")
        # return uncached_df.to_dict(orient="records")
    
    @task(task_id="stage_batches_bq", retries=0, retry_delay=timedelta(minutes=0))
    def stage_batches_to_bq(records, batch_size: int = BATCH_SIZE) -> list[str]:
        """Chunks records and inserts them into BQ with unique batch_id"""
        # Spark version for backfill
        spark = create_spark_session()
        print(f"[STAGE] Reading uncached parquet from {records}")
        df = spark.read.parquet(records)

        count = df.count()
        if count == 0:
            print("[STAGE] No rows to stage.")
            return []

        # Assign row index and compute batch group
        df_with_id = df.withColumn("row_num", monotonically_increasing_id())
        df_with_batch = df_with_id.withColumn("batch_index", floor(col("row_num") / batch_size))

        # Get distinct batch indices to generate unique UUIDs
        batch_indices = df_with_batch.select("batch_index").distinct().rdd.flatMap(lambda x: x).collect()
        batch_uuid_map = {idx: str(uuid.uuid4()) for idx in batch_indices}

        def assign_batch_id(index):
            return batch_uuid_map.get(index, str(uuid.uuid4()))

        assign_batch_id_udf = udf(assign_batch_id, StringType())
        final_df = df_with_batch.withColumn("batch_id", assign_batch_id_udf(col("batch_index"))).drop("row_num", "batch_index")

        print(f"[STAGE] Prepared {final_df.count()} rows with {len(batch_uuid_map)} unique batch_ids")

        # Write to BigQuery
        final_df.write \
            .format("bigquery") \
            .option("table", f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches") \
            .option("temporaryGcsBucket", TEMP_BUCKET) \
            .mode("append") \
            .save()

        spark.stop()
        return list(batch_uuid_map.values())
    
        # Pandas version (without spark)
        # bq_client = bigquery.Client()
        # staging_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches"
        # batch_ids = []

        # for i in range(0, len(records), batch_size):
        #     chunk = records[i:i+batch_size]
        #     batch_id = str(uuid.uuid4())
        #     for r in chunk:
        #         r["batch_id"] = batch_id
        #     df = pd.DataFrame(chunk)
        #     bq_client.load_table_from_dataframe(df, staging_table).result()
        #     batch_ids.append(batch_id)

        # print(f"[STAGE] Created {len(batch_ids)} batches of size ~{batch_size}")
        # return batch_ids

    @task(task_id="fetch_urls", retries=3, retry_delay=timedelta(minutes=3))
    def fetch_urls(batch_id: str):
        print("[FETCH] Fetching URLs in batch")
        client = bigquery.Client()
        staging_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches"
        df = client.query(f"""
            SELECT * FROM `{staging_table}` WHERE batch_id = '{batch_id}'
        """).to_dataframe()

        urls = df["page_url"].tolist()
        page_texts = asyncio.run(fetch_all_pages(urls, max_concurrent=2))

        df["page_text"] = df["page_url"].apply(
            lambda url: extract_visible_text(page_texts.get(url, ""))
        )
        # Overwrite existing rows for this batch
        job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
        )

        # Limit scope to this batch
        temp_table = f"{staging_table}_temp_{batch_id.replace('-', '_')}"

        print("[FETCH] Fetched page text for URLs")

        client.load_table_from_dataframe(df, staging_table).result()
        return batch_id 
        # return df.to_dict(orient="records")
        # print("[FETCH] Fetching URLs in batch")
        # urls = [x["page_url"] for x in batch_chunk]
        # page_texts = asyncio.run(fetch_all_pages(urls))
        # for x in batch_chunk:
        #     html = page_texts.get(x["page_url"], "")
        #     x["page_text"] = extract_visible_text(html) if html else ""
        # print(f"[FETCH] Fetched page text for {len(batch_chunk)} URLs")

        # import json
        # payload = json.dumps(batch_chunk)
        # print(f"[FETCH] Payload size: {len(payload)} bytes")

        # return batch_chunk

    @task(task_id="categorize_urls", retries=3, retry_delay=timedelta(minutes=3), max_active_tis_per_dag=5)
    def categorize(batch_with_texts):
        print("[CATEGORIZE] Starting URL categorization")
        #df_page_text = pd.DataFrame(batch_with_texts)
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches"
        df = client.query(f"""
            SELECT * FROM `{table_id}`
            WHERE batch_id = '{batch_with_texts}' AND page_text IS NOT NULL
        """).to_dataframe()

        categorized_df = categorize_urls(df)
        print(f"[CATEGORIZE] Categorized {len(categorized_df)} URLs")
        return categorized_df.to_dict(orient="records")
        # temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/categorized.csv"
        # categorized_df.to_csv(temp_path, index=False)
        # print(f"[CATEGORIZE] Categorized data written to: {temp_path}")
        # return temp_path

    @task(task_id="load_data", retries=0, retry_delay=timedelta(minutes=0))
    def load(batch_rows):
        print("[LOAD] Loading categorized data into BigQuery")
        # categorized_pd_df = pd.read_csv(categorized_path)
        # if categorized_pd_df.empty:
        #     print("[WARNING] No data to load. DataFrame is empty.")
        #     return "No data loaded"
        # spark = create_spark_session()
        # new_data = spark.createDataFrame(batch_rows)
        # load_data(new_data)
        # spark.stop()
        if not batch_rows:
            print("[LOAD] No data to load.")
            return "Skipped"
        
        df = pd.DataFrame(batch_rows)
        # Convert string timestamps back to datetime for BigQuery
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["last_accessed"] = pd.to_datetime(df["last_accessed"], errors="coerce")

        load_data(df)
        print("[LOAD] Data loaded successfully")
        return "Loaded"

    raw_path = extract()
    transformed_path = transform(raw_path)
    new_records = filter_cached_urls(transformed_path)
    batch_ids = stage_batches_to_bq(new_records)

    fetched = fetch_urls.expand(batch_id=batch_ids)
    categorized = categorize.expand(batch_with_texts=fetched)
    load.expand(batch_rows=categorized)

url_content_backfill()

