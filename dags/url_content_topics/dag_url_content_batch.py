from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import sys

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
    @task(task_id="extract_data")
    def extract():
        spark = create_spark_session()
        from config.project_config import PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID
        raw_df = extract_data(spark, PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID)
        spark.stop()
        return raw_df

    @task(task_id="transform_data")
    def transform(raw_df):
        spark = create_spark_session()
        transformed_df = transform_data(raw_df)
        spark.stop()
        return transformed_df

    @task(task_id="categorize_urls")
    def categorize(transformed_df):
        categorized_pd_df = categorize_urls(transformed_df.toPandas())
        return categorized_pd_df

    @task(task_id="load_data")
    def load(categorized_pd_df):
        spark = create_spark_session()
        categorized_df = spark.createDataFrame(categorized_pd_df)
        load_data(categorized_df)
        spark.stop()
        return "Loaded"

    raw_df = extract()
    transformed_df = transform(raw_df)
    categorized_pd_df = categorize(transformed_df)
    load(categorized_pd_df)

url_content_batch()

