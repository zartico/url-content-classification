from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable


from pyspark.sql.functions import monotonically_increasing_id, floor, col, lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from google.cloud import bigquery
from datetime import timedelta, datetime, timezone
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

BATCH_SIZE = 100
TOTAL_URLS = 2000
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
    max_active_runs=1,
)
def url_content_backfill():
    @task(task_id="make_run_id", retries=0, retry_delay=timedelta(minutes=0))
    def make_run_id() -> str:
        # Stable id for this DAG run (readable + unique)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{ts}_{uuid.uuid4().hex[:8]}"

    @task(task_id="extract_data", retries=0, retry_delay=timedelta(minutes=0), pool = "spark")
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

    @task(task_id="transform_data", retries=0, retry_delay=timedelta(minutes=0), pool = "spark")
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


    @task(task_id="filter_urls", retries=0, retry_delay=timedelta(minutes=0), pool = "spark")
    def filter_cached_urls(transformed_path, run_id: str):
        # Spark version for backfill
        print("[FILTER] Start filtering cached URLs")
        spark = create_spark_session()
        df = spark.read.parquet(transformed_path)
        print(f"[FILTER] Loaded from transformed data with {df.count()} rows")
        #uncached_df = filter_cache_spark(spark, df)
        uncached_df = filter_cache_spark(
            spark, df,
            run_id=run_id,
            include_staging_check=True,
            staging_table=f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches",
            temp_gcs_bucket=TEMP_BUCKET
        )
        print(f"[FILTER] Filtered cached URLs, remaining {uncached_df.count()} uncached rows")
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/uncached.parquet"
        uncached_df.write.mode("overwrite").parquet(temp_path)
        spark.stop()
        return temp_path
    
    @task(task_id="stage_batches_bq", retries=0, pool="spark")
    def stage_batches_to_bq(uncached_parquet_path: str, batch_size: int, run_id: str) -> list[str]:
        """
        1) Read uncached parquet (must include: url_hash, trimmed_page_url, site, page_url, client_id, access_hits).
        2) Assign deterministic batches of ~batch_size using row_number() over url_hash ordering.
        3) Write to a per-run temp BQ table.
        4) MERGE insert-if-missing into staging_url_batches keyed by (run_id, url_hash).
        5) Return the list of batch_ids that were staged.
        """
        spark = create_spark_session()

        print(f"[STAGE] Reading uncached parquet from {uncached_parquet_path}")
        df = spark.read.parquet(uncached_parquet_path)

        required_cols = {"url_hash", "trimmed_page_url", "site", "page_url", "client_id", "access_hits"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"[STAGE] Missing required columns in uncached dataset: {missing}")

        # Deterministic row numbers → batch_index
        w = Window.orderBy(F.col("url_hash").asc())
        df_idx = (df
                .withColumn("row_num", F.row_number().over(w))
                .withColumn("batch_index", F.floor((F.col("row_num") - F.lit(1)) / F.lit(batch_size)))
                )

        # Build (batch_index -> batch_id) mapping on driver; small cardinality so collect is fine
        batch_indices = [r.batch_index for r in df_idx.select("batch_index").distinct().collect()]
        batch_map_rows = [(int(i), f"batch_{i}_{run_id}") for i in sorted(batch_indices)]
        batch_map_df = spark.createDataFrame(batch_map_rows, schema=T.StructType([
            T.StructField("batch_index", T.IntegerType(), False),
            T.StructField("batch_id",    T.StringType(),  False),
        ]))

        # Join mapping -> add run_id
        staged_df = (df_idx.join(batch_map_df, on="batch_index", how="left")
                        .drop("row_num", "batch_index")
                        .withColumn("run_id", F.lit(run_id))
                    )

        # Write to per-run temp table in BigQuery
        tmp_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.tmp_stage_candidates_{run_id.replace('-', '_')}"
        (staged_df
            .select("url_hash", "trimmed_page_url", "site", "page_url", "client_id", "access_hits", "run_id", "batch_id")
            .write
            .format("bigquery")
            .option("table", tmp_table)
            .option("temporaryGcsBucket", TEMP_BUCKET)
            .mode("overwrite")
            .save())
        print(f"[STAGE] Wrote candidates to {tmp_table}")

        # MERGE insert-if-missing into staging_url_batches (race-safe)
        client = bigquery.Client(project=PROJECT_ID)
        staging_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches"

        merge_sql = f"""
        MERGE `{staging_table}` T
        USING `{tmp_table}` S
        ON T.run_id = S.run_id AND T.url_hash = S.url_hash
        WHEN NOT MATCHED THEN
        INSERT (url_hash, trimmed_page_url, site, page_url, client_id, access_hits, run_id, batch_id)
        VALUES (S.url_hash, S.trimmed_page_url, S.site, S.page_url, S.client_id, S.access_hits, S.run_id, S.batch_id)
        """
        client.query(merge_sql).result()
        print(f"[STAGE] MERGE complete into {staging_table}")

        # Get the finalized batch_ids for this run (distinct)
        q = client.query(f"""
        SELECT DISTINCT batch_id
        FROM `{staging_table}`
        WHERE run_id = @run_id
        """, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
        ))
        batch_ids = [r["batch_id"] for r in q.result()]
        print(f"[STAGE] Created {len(batch_ids)} batches for run_id={run_id}")

        # Optional: drop temp table to keep dataset clean
        client.query(f"DROP TABLE `{tmp_table}`").result()
        print(f"[STAGE] Dropped temp table {tmp_table}")

        spark.stop()
        return batch_ids

    @task(task_id="fetch_urls", retries=3, retry_delay=timedelta(minutes=3), max_active_tis_per_dag=10, pool = "fetch")
    def fetch_urls(batch_id: str):
        print("[FETCH] Fetching URLs in batch")
        client = bigquery.Client()
        staging_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches"
        df = client.query(f"""
            SELECT * FROM `{staging_table}` WHERE batch_id = '{batch_id}'
        """).to_dataframe()

        urls = df["page_url"].tolist()
        page_texts = asyncio.run(fetch_all_pages(urls, max_concurrent=100, limit_per_host=3))

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

    @task(task_id="categorize_urls", retries=3, retry_delay=timedelta(minutes=3), max_active_tis_per_dag=14, pool = "nlp", trigger_rule=TriggerRule.ALL_DONE)
    def categorize(batch_id: str):
        print("[CATEGORIZE] Starting URL categorization")
        #df_page_text = pd.DataFrame(batch_with_texts)
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_batches"
        df = client.query(f"""
            SELECT * FROM `{table_id}`
            WHERE batch_id = '{batch_id}' AND page_text IS NOT NULL
        """).to_dataframe()

        if df.empty:
            print(f"[CATEGORIZE] No page_text for batch_id={batch_id}; skipping.")
            return []

        categorized_df = categorize_urls(df)
        print(f"[CATEGORIZE] Categorized {len(categorized_df)} URLs")
        return categorized_df.to_dict(orient="records")


    @task(task_id="load_data", retries=0, retry_delay=timedelta(minutes=0))
    def load(batch_rows):
        print("[LOAD] Loading categorized data into BigQuery")

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

    run_id = make_run_id()
    raw_path = extract()
    transformed_path = transform(raw_path)
    new_records = filter_cached_urls(transformed_path, run_id=run_id)
    batch_ids = stage_batches_to_bq(uncached_parquet_path=new_records, batch_size=BATCH_SIZE, run_id=run_id)

    fetched = fetch_urls.expand(batch_id=batch_ids)
    categorized = categorize.expand(batch_id=batch_ids)
    # Categorize waits for corresponding fetch to complete
    fetched >> categorized
    load.expand(batch_rows=categorized)

url_content_backfill()

