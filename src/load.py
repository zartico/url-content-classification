from pyspark.sql import DataFrame
from google.cloud import bigquery
from config.project_config import PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID
from google.api_core.exceptions import NotFound
import time


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
            bigquery.SchemaField("trimmed_page_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("site", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("page_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("content_topic", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("prediction_confidence", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("review_flag", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("client_id", "STRING", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"[INFO] Created table {BQ_TABLE_ID}.")
    
    return table_ref

def load_data(df):
    """ Load data into BigQuery using Spark BigQuery connector."""

    table_ref = ensure_dataset_and_table_exist()
    if table_ref is None:
        return

    # Select only required columns
    df_final = df.select(
        "trimmed_page_url",
        "site", 
        "page_url",
        "content_topic",
        "prediction_confidence",
        "review_flag",
        "client_id"
    )
    
    # Write to BigQuery
    df_final.write \
        .format("bigquery") \
        .option("table", f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()
    
    print(f"[INFO] Loaded {df_final.count()} rows to {BQ_TABLE_ID}")