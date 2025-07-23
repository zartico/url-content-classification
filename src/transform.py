from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lower, trim, lit, concat, concat_ws
)

def transform_data(df: DataFrame) -> DataFrame:
    """
    Build full URLs using Spark logic instead of Python UDF.
    Clean and deduplicate the dataset.
    """
    invalids = ["(not set)", "none", "null", ""]

    df_cleaned = df.withColumn("trimmed_page_url", trim(lower(col("trimmed_page_url")))) \
        .withColumn("site", trim(lower(col("site"))))

    df_filtered = df_cleaned.filter(
        ~col("trimmed_page_url").isin(invalids) & ~col("site").isin(invalids)
    )

    df_transformed = df_filtered.withColumn(
        "page_url",
        when(col("trimmed_page_url").startswith("http"), col("trimmed_page_url").substr(1, 999))
        .when(col("trimmed_page_url").startswith("//"), concat(lit("https:"), col("trimmed_page_url")))
        .when(col("trimmed_page_url").startswith("/"),
              concat(lit("https://"), col("site"), col("trimmed_page_url")))
        .otherwise(concat(lit("https://"), col("trimmed_page_url")))
    )

    df_result = df_transformed.filter(col("page_url").isNotNull()) \
        .dropDuplicates(["page_url", "client_id"])

    return df_result
