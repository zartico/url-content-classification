from google.cloud import bigquery
import pandas as pd


def extract_data(spark, project_id: str, dataset_id: str, table_id: str, 
                 date_start: str | None = None, date_end: str | None = None):
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

    date_filter = ""
    if date_start and date_end:
        date_filter = f"AND date >= DATE('{date_start}') AND date < DATE('{date_end}')"
    elif date_start:
        date_filter = f"AND date = DATE('{date_start}')"

    query = f"""
    WITH base AS (
      SELECT
        LOWER(TRIM(site)) AS site,
        LOWER(TRIM(trimmed_page_url)) AS trimmed_page_url,
        client_id
      FROM `{project_id}.{dataset_id}.{table_id}`
      WHERE trimmed_page_url IS NOT NULL
        AND site IS NOT NULL
        AND client_id NOT LIKE '%Demo%'
        {date_filter}
    ),
    enriched AS (
      SELECT
        site,
        trimmed_page_url,
        client_id,
        TO_HEX(SHA256(CAST(trimmed_page_url AS STRING))) AS url_hash
      FROM base
    ),
    counts AS (
      SELECT url_hash, COUNT(*) AS dup_hits
      FROM enriched
      GROUP BY url_hash
    ),
    reps AS (
      SELECT * EXCEPT(rn) FROM (
        SELECT
          site, trimmed_page_url, client_id, url_hash,
          ROW_NUMBER() OVER (
            PARTITION BY url_hash
            ORDER BY LENGTH(trimmed_page_url) ASC, trimmed_page_url ASC
          ) AS rn
        FROM enriched
      )
      WHERE rn = 1
    )
    SELECT
      reps.site,
      reps.trimmed_page_url,
      reps.client_id,
      reps.url_hash,
      counts.dup_hits AS access_hits
    FROM reps
    JOIN counts USING (url_hash)
    """

    df = spark.read \
        .format("bigquery") \
        .option("query", query) \
        .option("parentProject", project_id) \
        .option("viewsEnabled", "true") \
        .option("materializationDataset", dataset_id) \
        .load()

    return df

