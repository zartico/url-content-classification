from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from google.cloud import bigquery
from datetime import timedelta, datetime, timezone
from math import ceil
import pandas as pd
import asyncio
import uuid
import pytz
import sys

sys.path.append('/home/airflow/gcs/data/url_content_topics')
sys.path.append('/home/airflow/gcs/data/url_content_topics/src')
sys.path.append('/home/airflow/gcs/data/url_content_topics/utils')
sys.path.append('/home/airflow/gcs/data/url_content_topics/config')

from config.project_config import PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID, BQ_TABLE_ID, BQ_DATASET_ID
from src.load import load_data
from src.extract import extract_data
from src.transform import transform_data
from utils.cache import filter_cache_spark
from utils.web_fetch import fetch_all_pages, extract_visible_text
from src.categorize import categorize_urls
from src.load import load_data

# Use the GCS bucket configured in Airflow Variables
TEMP_BUCKET = Variable.get("TEMP_GCS_BUCKET")  # e.g. non-codecs-prod-airflow/data/tmp/spark_staging
URL_CONTENT_SKIP_RECONCILE = Variable.get("URL_CONTENT_SKIP_RECONCILE", default_var = "0") # lifetime view_count from GA4 table
SKIP_RECONCILE_BOOL_DEFAULT = (URL_CONTENT_SKIP_RECONCILE == "1")

# --- DAG PARAMS ---
default_end = datetime.now().astimezone(pytz.timezone('US/Central'))
default_start = datetime.now().astimezone(pytz.timezone('US/Central')) - timedelta(days=10)

default_end = default_end.strftime("%Y-%m-%d")
default_start = default_start.strftime("%Y-%m-%d")

dag_params = {
    "start_date": Param(
        str(default_start), 
        title="Start Date (YYYY-MM-DD)", 
        type="string", 
        format="date"
    ),
    "end_date": Param(
        str(default_end), 
        title="End Date (YYYY-MM-DD)", 
        type="string", 
        format="date"
    ),
    "batch_size": Param(
        1000,          
        title="Batch size (URLs per batch)", 
        type="integer"
    ),
    "max_batches_cap": Param(
        900,      
        title="Max batches cap (safety)", 
        type="integer"
    ),
    "skip_reconcile": Param(
        False, 
        title="Skip Reconcile in Load", 
        type="boolean"
    ),
}

# --- SPARK SETUP ---
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
    schedule_interval="30 20 * * *", 
    start_date=days_ago(1),
    catchup=False,
    tags=["url-content"],
    max_active_runs=1,
    params=dag_params,
    render_template_as_native_obj=True,
)
def url_content_daily():

    @task(task_id="resolve_config", multiple_outputs=True)
    def resolve_config(params: dict):
        # Read runtime context
        ctx = get_current_context()
        params = dict(ctx.get("params", {}))                      # values from DAG params (UI defaults)
        conf = dict((ctx.get("dag_run").conf or {}))              # per-run overrides (Trigger DAG w/ config)

        # Dynamic fallbacks
        central = pytz.timezone('US/Central')
        today = datetime.now().astimezone(central).date()
        fallback_end = today.strftime("%Y-%m-%d")
        fallback_start = (today - timedelta(days=10)).strftime("%Y-%m-%d")

        # Getters: dag_run.conf overrides params and overrides runtime fallback
        def get_val(key, default):
            return conf.get(key, params.get(key, default))

        start = get_val("start_date", fallback_start)
        end   = get_val("end_date",   fallback_end)
        bsz   = int(get_val("batch_size", 1000))          
        cap   = int(get_val("max_batches_cap", 900))
        skip  = bool(get_val("skip_reconcile", SKIP_RECONCILE_BOOL_DEFAULT))

        return {
            "start_date": start, 
            "end_date": end, 
            "batch_size": bsz,
            "max_batches_cap": cap, 
            "skip_reconcile": skip
        }

    @task(task_id="make_run_id", retries=3, retry_delay=timedelta(minutes=3))
    def make_run_id() -> str:
        # Stable id for this DAG run (readable + unique)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{ts}_{uuid.uuid4().hex[:8]}"

    @task(task_id="extract_data", retries=3, retry_delay=timedelta(minutes=3), pool = "spark")
    def extract(cfg: dict):
        print("[EXTRACT] Starting data extraction from BigQuery")
        spark = create_spark_session()
        
        raw_df = extract_data(
            spark,
            PROJECT_ID, 
            GA4_DATASET_ID, 
            GA4_TABLE_ID,
            date_start=cfg["start_date"],
            date_end=cfg["end_date"]
        )

        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/raw.parquet"
        raw_df.write.mode("overwrite").parquet(temp_path)
        print(f"[EXTRACT] Extracted {raw_df.count()} distinct from BigQuery")

        spark.stop()
        return temp_path

    @task(task_id="transform_data", retries=3, retry_delay=timedelta(minutes=3), pool = "spark")
    def transform(raw_path):
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


    @task(task_id="filter_urls", retries=3, retry_delay=timedelta(minutes=3), pool = "spark")
    def filter_cached_urls(transformed_path, run_id: str):
        print("[FILTER] Start filtering cached URLs")
        spark = create_spark_session()
        df = spark.read.parquet(transformed_path)

        print(f"[FILTER] Loaded from transformed data with {df.count()} rows")
        uncached_df = filter_cache_spark(
            spark, df,
            run_id=run_id,
            include_staging_check=True,
            staging_table=f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_info",
            temp_gcs_bucket=TEMP_BUCKET
        )

        print(f"[FILTER] Filtered cached URLs, remaining {uncached_df.count()} uncached rows")
        temp_path = "/home/airflow/gcs/data/url_content_topics/tmp/uncached.parquet"
        uncached_df.write.mode("overwrite").parquet(temp_path)
        spark.stop()
        return temp_path
    
    @task(task_id="stage_batches_bq", retries=3, retry_delay=timedelta(minutes=3), pool="spark")
    def stage_batches_to_bq(uncached_parquet_path: str, batch_size: int, max_batches_cap: int, run_id: str) -> list[str]:
        """
        Stage uncached URLs into batches for downstream processing.

        Steps:
        1) Read uncached parquet (must include: url_hash, trimmed_page_url, site, page_url, client_id, access_hits)
        2) Compute batches by a fixed batch_size (# of URLs per batch, default around 1k URLs)
            - If the resulting number of batches would exceed max_batches_cap,
            automatically increase batch_size so the cap is respected
        3) Write staged candidates to a per-run temporary BQ table
        4) MERGE into staging_url_info (upsert by run_id + url_hash)
        5) Drop temp table
        6) Return list of batch_ids
        """
        spark = create_spark_session()

        print(f"[STAGE] Reading uncached parquet from {uncached_parquet_path}")
        df = spark.read.parquet(uncached_parquet_path)

        required_cols = {"url_hash", "trimmed_page_url", "site", "page_url", "client_id", "access_hits"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"[STAGE] Missing required columns in uncached dataset: {missing}")

        uncached_count = df.count()
        if uncached_count == 0:
            print("[STAGE] No uncached rows; nothing to stage.")
            spark.stop()
            return []

        # Compute batches by size, then enforce cap
        requested_batches = ceil(uncached_count / max(1, batch_size))
        if requested_batches > max_batches_cap:
            # Inflate batch_size to respect the cap
            batch_size = ceil(uncached_count / max_batches_cap)
            print(f"[STAGE] requested_batches={requested_batches} > cap={max_batches_cap}; "
                f"inflating batch_size to {batch_size}")
        else:
            print(f"[STAGE] Using requested batch_size={batch_size} (batches≈{requested_batches})")

        # Deterministic row numbers → batch_index
        w = Window.orderBy(F.col("url_hash").asc())
        df_idx = (df
                .withColumn("row_num", F.row_number().over(w))
                .withColumn("batch_index", F.floor((F.col("row_num") - F.lit(1)) / F.lit(batch_size)))
                )

        # Build (batch_index -> batch_id) mapping 
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

        # MERGE insert-if-missing into staging_url_info (race-safe)
        client = bigquery.Client(project=PROJECT_ID)
        staging_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_info"

        merge_sql = f"""
        MERGE `{staging_table}` T
        USING `{tmp_table}` S
        ON T.run_id = S.run_id AND T.url_hash = S.url_hash
        WHEN MATCHED THEN
        UPDATE SET T.access_hits = S.access_hits
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

        # Drop temp table to keep dataset clean
        client.query(f"DROP TABLE `{tmp_table}`").result()
        print(f"[STAGE] Dropped temp table {tmp_table}")

        spark.stop()
        return batch_ids

    @task_group(group_id="process_batch")
    def process_batch_group(batch_id: str, skip_reconcile: bool):
        """
        Per-batch sub-DAG: fetch -> categorize -> load
        Using a TaskGroup lets batches complete independently under resource pressure.
        """

        @task(task_id="fetch_urls", 
              retries=3, 
              retry_delay=timedelta(minutes=3),
              execution_timeout=timedelta(minutes=60),
              max_active_tis_per_dag=10, 
              pool="fetch")
        def fetch_urls(batch_id: str) -> str:
            print("[FETCH] Fetching URLs in batch")
            client = bigquery.Client()
            staging_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_info"

            # Read this batch
            df = client.query(
                f"""
                    SELECT batch_id, url_hash, page_url
                    FROM `{staging_table}`
                    WHERE batch_id = @batch_id
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
                    )
            ).to_dataframe()

            if df.empty:
                print(f"[FETCH] No rows for batch_id={batch_id}")
                return batch_id

            # Fetch HTML/text (adjust concurrency as needed)
            page_texts = asyncio.run(
                fetch_all_pages(
                    df["page_url"].tolist(),
                    max_concurrent=8,
                    limit_per_host=2,
                )
            )
            # Parse HTML to visible text
            df["page_text"] = df["page_url"].apply(lambda u: extract_visible_text(page_texts.get(u, "")))

            # Write to per-batch temp table then MERGE into staging
            tmp_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.tmp_fetch_{batch_id.replace('-', '_')}"
            payload = df[["batch_id", "url_hash", "page_url", "page_text"]]

            client.load_table_from_dataframe(payload, tmp_table).result()
            print(f"[FETCH] Wrote {len(payload)} rows to {tmp_table}")

            merge_sql = f"""
            MERGE `{staging_table}` T
            USING `{tmp_table}` S
            ON T.batch_id = S.batch_id AND T.url_hash = S.url_hash
            WHEN MATCHED THEN
            UPDATE SET
                T.page_text = S.page_text
            """
            client.query(merge_sql).result()
            client.query(f"DROP TABLE `{tmp_table}`").result()
            print(f"[FETCH] MERGE complete for batch_id={batch_id}")
            return batch_id

        @task(task_id="categorize_urls", 
              retries=3, 
              retry_delay=timedelta(minutes=3),
              execution_timeout=timedelta(minutes=40),
              max_active_tis_per_dag=14, 
              pool="nlp",
              trigger_rule=TriggerRule.ALL_DONE)
        def categorize(batch_id: str):
            print("[CATEGORIZE] Starting URL categorization")
            client = bigquery.Client()
            table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.staging_url_info"

            df = client.query(
                f"""
                SELECT *
                FROM `{table_id}`
                WHERE batch_id = @batch_id AND page_text IS NOT NULL
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
                )
            ).to_dataframe()

            if df.empty:
                print(f"[CATEGORIZE] No page_text for batch_id={batch_id}; skipping.")
                return []

            categorized_df = categorize_urls(df) 
            print(f"[CATEGORIZE] Categorized {len(categorized_df)} URLs in batch_id={batch_id}")
            return categorized_df.to_dict(orient="records")

        @task(task_id="load_data", retries=3, retry_delay=timedelta(minutes=3))
        def load(batch_rows, skip_reconcile: bool):
            print("[LOAD] Loading categorized data into BigQuery")
            if not batch_rows:
                print("[LOAD] No data to load.")
                return "Skipped"
            
            df = pd.DataFrame(batch_rows)
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
            df["last_accessed"] = pd.to_datetime(df["last_accessed"], errors="coerce")

            load_data(df, skip_reconcile=skip_reconcile)
            print("[LOAD] Data loaded successfully")
            return "Loaded"

        fetched = fetch_urls(batch_id)
        categorized = categorize(batch_id)
        loaded = load(categorized, skip_reconcile)

        fetched >> categorized >> loaded


    # --- MAIN DAG FLOW ---
    cfg = resolve_config()
    run_id = make_run_id()

    raw_path = extract(cfg)
    transformed_path = transform(raw_path)
    new_records = filter_cached_urls(transformed_path, run_id=run_id)
    
    batch_ids = stage_batches_to_bq(
        uncached_parquet_path=new_records,
        batch_size=cfg["batch_size"],           
        max_batches_cap=cfg["max_batches_cap"],  
        run_id=run_id,
    )

    process_batch_group.partial(skip_reconcile=cfg["skip_reconcile"]).expand(
        batch_id=batch_ids
    )

url_content_daily()

