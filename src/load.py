from pyspark.sql import DataFrame
from pyspark.sql.types import *
from google.cloud import bigquery
from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
from google.api_core.exceptions import NotFound
import pandas as pd


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
        return None  # Exit early

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

def load_data(df: pd.DataFrame): 
    """ Load data into BigQuery using Spark BigQuery connector. """
    table_ref = ensure_dataset_and_table_exist()
    if table_ref is None or df.empty:
        print("[LOAD] No data loaded (empty DataFrame or table missing).")
        return
    
    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(df, table_ref).result()
    print(f"[INFO] Loaded {len(df)} rows to {BQ_TABLE_ID}")


# def load_data(df: DataFrame): # SPARK DEPRECATED
#     """ Load data into BigQuery using Spark BigQuery connector. """
#     table_ref = ensure_dataset_and_table_exist()
#     if table_ref is None:
#         return

#     # Enforce correct column order
#     ordered_columns = [
#         "url_hash", "created_at", "trimmed_page_url", "site", "page_url",
#         "zartico_category", "content_topic", "prediction_confidence", "review_flag",
#         "nlp_raw_categories", "client_id", "last_accessed", "view_count"
#     ]
#     df_reordered = df.select(*ordered_columns)

#     # Explicit casting if necessary
#     df_casted = df_reordered.select(
#         df_reordered.url_hash.cast("string"),
#         df_reordered.created_at.cast("timestamp"),
#         df_reordered.trimmed_page_url.cast("string"),
#         df_reordered.site.cast("string"),
#         df_reordered.page_url.cast("string"),
#         df_reordered.zartico_category.cast("string"),
#         df_reordered.content_topic.cast("string"),
#         df_reordered.prediction_confidence.cast("double"),
#         df_reordered.review_flag.cast("boolean"),
#         df_reordered.nlp_raw_categories.cast("string"),
#         df_reordered.client_id.cast("string"),
#         df_reordered.last_accessed.cast("timestamp"),
#         df_reordered.view_count.cast("integer")
#     )

#     # Write to BigQuery
#     df_casted.write \
#         .format("bigquery") \
#         .option("table", f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}") \
#         .option("writeMethod", "direct") \
#         .mode("append") \
#         .save()

#     print(f"[INFO] Loaded {df_casted.count()} rows to {BQ_TABLE_ID}")

