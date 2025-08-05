from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import timedelta
from google.cloud import bigquery
import pandas as pd
import asyncio
import uuid
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
from utils.cache import filter_cache
from utils.web_fetch import fetch_all_pages, extract_visible_text
from src.categorize import categorize_urls
from src.load import load_data

BATCH_SIZE = 50
TOTAL_URLS = 200
MAX_DYNAMIC_TASKS = 500

def create_spark_session():
    """Create Spark session with BigQuery connector"""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("BigQueryIntegration") \
        .config("spark.jars.packages", ",".join([
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0",
            "javax.inject:javax.inject:1",
            "org.scala-lang:scala-library:2.12.18"
        ])) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["url-content"],
)
def url_content():
    @task(task_id="extract_data", retries=0, retry_delay=timedelta(minutes=0))
    def extract():
        print("[EXTRACT] Starting data extraction from BigQuery")
        #spark = create_spark_session()
        raw_df = extract_data(PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID, TOTAL_URLS)
        print(f"[EXTRACT] Extracted {len(raw_df)} rows from BigQuery")
        return raw_df.to_dict(orient="records")
        #temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/raw.parquet"
        #raw_df.to_parquet(temp_path, index=False)
        #raw_df.write.mode("overwrite").parquet(temp_path)
        #spark.stop()
        #return temp_path

    @task(task_id="transform_data", retries=0, retry_delay=timedelta(minutes=0))
    def transform(raw_path):
        print("[TRANSFORM] Starting data transformation")
        #spark = create_spark_session()
        #raw_df = spark.read.parquet(raw_path)
        #raw_df = pd.read_parquet(raw_path)
        raw_df = pd.DataFrame(raw_path)
        print(f"[TRANSFORM] Raw DataFrame loaded with {len(raw_df)} rows")
        transformed_df = transform_data(raw_df)
        print(f"[TRANSFORM] Transformed/Cleaned {len(transformed_df)} rows")
        return transformed_df.to_dict(orient="records")
        #temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/transformed.parquet"
        #transformed_df.to_parquet(temp_path, index=False)
        #transformed_df.write.mode("overwrite").parquet(temp_path)
        #spark.stop()
        #return temp_path

    @task(task_id="filter_urls", retries=0, retry_delay=timedelta(minutes=0))
    def filter_cached_urls(transformed_path):
        print("[FILTER] Start filtering cached URLs")
        #spark = create_spark_session()
        #df = spark.read.parquet(transformed_path).toPandas()
        #spark.stop()
        #df = pd.read_parquet(transformed_path)
        df = pd.DataFrame(transformed_path)
        print(f"[FILTER] Loaded from transformed data with {len(df)} rows")
        uncached_df = filter_cache(df)
        print(f"[FILTER] Filtered cached URLs, remaining {len(uncached_df)} uncached rows")
        return uncached_df.to_dict(orient="records")
    
    # @task(task_id="chunk_urls", retries=0, retry_delay=timedelta(minutes=0))
    # def chunk(batch: list, chunk_size: int = 5):
    #     """Yield successive n-sized chunks from batch."""
    #     print(f"[CHUNK] Splitting batch of {len(batch)} into chunks of size {chunk_size}")
    #     chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
    #     print(f"[CHUNK] Created {len(chunks)} chunks")
    #     return chunks
    
    @task(task_id="stage_batches_bq", retries=0, retry_delay=timedelta(minutes=0))
    def stage_batches_to_bq(records, chunk_size: int = 5) -> list[str]:
        """Chunks records and inserts them into BQ with unique batch_id"""
        bq_client = bigquery.Client()
        staging_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches"
        batch_ids = []

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i+chunk_size]
            batch_id = str(uuid.uuid4())
            for r in chunk:
                r["batch_id"] = batch_id
            df = pd.DataFrame(chunk)
            bq_client.load_table_from_dataframe(df, staging_table).result()
            batch_ids.append(batch_id)

        return batch_ids

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

    @task(task_id="categorize_urls", retries=3, retry_delay=timedelta(minutes=3))
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
    #url_chunks = chunk(new_records, chunk_size=5)

    fetched = fetch_urls.expand(batch_id=batch_ids)
    categorized = categorize.expand(batch_with_texts=fetched)
    load.expand(batch_rows=categorized)

url_content()

