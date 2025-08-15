#from pyspark.sql import SparkSession
#from pyspark.sql import DataFrame
#from pyspark.sql.functions import col, lit
from google.cloud import bigquery
import pandas as pd


#def extract_data(spark, project_id: str, dataset_id: str, table_id: str, limit: int):
def extract_data(spark, project_id: str, dataset_id: str, table_id: str):
    """
    Extracts data from a BigQuery table using Spark BigQuery connector

    Args:
        project_id (str): The Google Cloud project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.

    Returns:
        DataFrame: The extracted data as a Spark DataFrame.
    """
    client = bigquery.Client(project=project_id)
    # Excluding Zartico demo 
    # For production, remove limit and date filter
    query = f"""
        SELECT DISTINCT site, trimmed_page_url, client_id 
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE trimmed_page_url IS NOT NULL
            AND site IS NOT NULL
            AND client_id NOT LIKE '%Demo%'
        """
    # df = client.query(query).to_dataframe()
    df = spark.read \
        .format("bigquery") \
        .option("query", query) \
        .option("parentProject", project_id) \
        .option("viewsEnabled", "true") \
        .option("materializationDataset", dataset_id) \
        .load()
        #.limit(limit)
    
    # print(f"[INFO] Extracted {len(df)} rows from BigQuery")
    # print(f"[INFO] Extracted {df.count()} rows from BigQuery")
    return df

