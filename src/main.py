import pyspark
print("PySpark Version:", pyspark.__version__)

from pyspark.sql import SparkSession
from .extract import extract_data
from .transform import transform_data
from .categorize import categorize_urls
from  .load import load_data
import pandas as pd
from config.project_config import PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID

def create_spark_session():
    """Create Spark session with BigQuery connector"""
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


def run():
    """ Main function to run the data extraction and processing. """

    # Create Spark session
    spark = create_spark_session()
    if not spark:
        print("[ERROR] Failed to create Spark session.")
        return

    try: 
        # Extract data from BigQuery
        raw_df = extract_data(spark, PROJECT_ID, GA4_DATASET_ID, GA4_TABLE_ID)
        print("Data extracted successfully:")
        raw_df.show(5)

        # Transform data
        transformed_df = transform_data(raw_df)
        print("Data transformed successfully:")
        transformed_df.show(5)

        # Categorize URLs
        categorized_df = categorize_urls(transformed_df)
        print("Data categorized successfully:")
        categorized_df.show(5)

        # Load data to BigQuery table
        load_data(categorized_df)
        print("Data loaded successfully to BigQuery.")

    except Exception as e:
        print(f"[ERROR] An error occurred: {e}")
    
    finally:
        spark.stop()


if __name__ == "__main__":
    run()   