from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import timedelta
import pandas as pd
import sys
import os

sys.path.append('/home/airflow/gcs/data/url_content_topics')
sys.path.append('/home/airflow/gcs/data/url_content_topics/src')
sys.path.append('/home/airflow/gcs/data/url_content_topics/utils')
sys.path.append('/home/airflow/gcs/data/url_content_topics/config')

from extract import extract_data
from transform import transform_data
from categorize import categorize_urls
from load import load_data

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
        spark = create_spark_session()
        from config.project_config import PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID
        raw_df = extract_data(spark, PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID)
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/raw.parquet"
        raw_df.write.mode("overwrite").parquet(temp_path)
        spark.stop()
        return temp_path

    @task(task_id="transform_data", retries=0, retry_delay=timedelta(minutes=0))
    def transform(raw_path):
        spark = create_spark_session()
        raw_df = spark.read.parquet(raw_path)
        transformed_df = transform_data(raw_df)
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/transformed.parquet"
        transformed_df.write.mode("overwrite").parquet(temp_path)
        spark.stop()
        return temp_path

    @task(task_id="categorize_urls", retries=0, retry_delay=timedelta(minutes=0))
    def categorize(transformed_path):
        spark = create_spark_session()
        transformed_df = spark.read.parquet(transformed_path)
        categorized_pd_df = categorize_urls(transformed_df.toPandas())
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/categorized.csv"
        categorized_pd_df.to_csv(temp_path, index=False)
        spark.stop()
        return temp_path

    @task(task_id="load_data", retries=0, retry_delay=timedelta(minutes=0))
    def load(categorized_path):
        categorized_pd_df = pd.read_csv(categorized_path)
        if categorized_pd_df.empty:
            print("[WARNING] No data to load. DataFrame is empty.")
            return "No data loaded"
        spark = create_spark_session()
        categorized_df = spark.createDataFrame(categorized_pd_df)
        load_data(categorized_df)
        spark.stop()
        return "Loaded"

    raw_path = extract()
    transformed_path = transform(raw_path)
    categorized_path = categorize(transformed_path)
    load(categorized_path)

url_content_batch()

