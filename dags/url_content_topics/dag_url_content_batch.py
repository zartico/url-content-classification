from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import timedelta
from google.cloud import bigquery
import pandas as pd
import asyncio
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
from utils.web_fetch import fetch_all_pages
from src.categorize import categorize_urls
from src.load import load_data

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
def url_content_batch():
    @task(task_id="extract_data", retries=0, retry_delay=timedelta(minutes=0))
    def extract():
        print("[EXTRACT] Starting data extraction from BigQuery")
        spark = create_spark_session()
        raw_df = extract_data(spark, PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID)
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/raw.parquet"
        raw_df.write.mode("overwrite").parquet(temp_path)
        spark.stop()
        print("[EXTRACT] Data written to: ", temp_path)
        return temp_path

    @task(task_id="transform_data", retries=0, retry_delay=timedelta(minutes=0))
    def transform(raw_path):
        print("[TRANSFORM] Starting data transformation")
        spark = create_spark_session()
        raw_df = spark.read.parquet(raw_path)
        print(f"[TRANSFORM] Raw DataFrame loaded with {raw_df.count()} rows")
        transformed_df = transform_data(raw_df)
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/transformed.parquet"
        transformed_df.write.mode("overwrite").parquet(temp_path)
        spark.stop()
        print("[TRANSFORM] Transformed DataFrame written to: ", temp_path)
        return temp_path

    @task(task_id="filter_urls", retries=0, retry_delay=timedelta(minutes=0))
    def filter_cached_urls(transformed_path):
        print("[FILTER] Start filtering cached URLs")
        spark = create_spark_session()
        df = spark.read.parquet(transformed_path).toPandas()
        spark.stop()
        print(f"[FILTER] Loaded from transformed data with {len(df)} rows")
        uncached_df = filter_cache(df)
        print(f"[FILTER] Filtered cached URLs, remaining {len(uncached_df)} uncached rows")
        return uncached_df.to_dict(orient="records")
    
    @task(task_id="chunk_urls", retries=0, retry_delay=timedelta(minutes=0))
    def chunk(batch: list, chunk_size: int = 100):
        """Yield successive n-sized chunks from batch."""
        print(f"[CHUNK] Splitting batch of {len(batch)} into chunks of size {chunk_size}")
        chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
        print(f"[CHUNK] Created {len(chunks)} chunks")
        return chunks

    @task(task_id="fetch_urls", retries=0, retry_delay=timedelta(minutes=0))
    def fetch_urls(batch_chunk):
        print("[FETCH] Fetching URLs in batch")
        urls = [x["page_url"] for x in batch_chunk]
        page_texts = asyncio.run(fetch_all_pages(urls))
        for x in batch_chunk:
            x["page_text"] = page_texts.get(x["page_url"], "")
        print(f"[FETCH] Fetched page text for {len(batch_chunk)} URLs")
        return batch_chunk

    @task(task_id="categorize_urls", retries=0, retry_delay=timedelta(minutes=0))
    def categorize(batch_with_texts):
        print("[CATEGORIZE] Starting URL categorization")
        df_page_text = pd.DataFrame(batch_with_texts)
        categorized_df = categorize_urls(df_page_text)
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
        spark = create_spark_session()
        categorized_df = spark.createDataFrame(batch_rows)
        new_data = spark.createDataFrame(categorized_df)
        load_data(new_data)
        spark.stop()
        print("[LOAD] Data loaded successfully")
        return "Loaded"

    raw_path = extract()
    transformed_path = transform(raw_path)
    new_records = filter_cached_urls(transformed_path)
    url_chunks = chunk(new_records, chunk_size=100)
    fetched = fetch_urls(batch_chunk=url_chunks)
    categorized = categorize.expand(batch_with_texts=fetched)
    load.expand(batch_rows=categorized)

url_content_batch()

