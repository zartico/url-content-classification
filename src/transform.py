from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace, lower, trim, udf
from pyspark.sql.types import StringType
from urllib.parse import urljoin
import re

def build_full_url(trimmed, site):
    if not trimmed or not site:
        return None

    trimmed = trimmed.strip().lower()
    site = site.strip().lower()

    # Filter invalid/ placeholder values
    invalid_values = {"(not set)", "none", "null", ""}
    if trimmed in invalid_values or site in invalid_values:
        return None

    # Already full URL, return cleaned
    if trimmed.startswith("http://") or trimmed.startswith("https://"):
        return trimmed.rstrip("/")

    # Handle double slashes
    if trimmed.startswith("//"):
        return f"https:{trimmed.rstrip('/')}"

    # Trimmed is path (starts with /), fallback to site column
    if trimmed.startswith("/"):
        return f"https://{site.rstrip('/')}{trimmed}"

    # Default: assume trimmed includes domain, prepend https://
    return f"https://{trimmed.rstrip('/')}"

# Register UDF
build_full_url_udf = udf(build_full_url, StringType())

def transform_data(df: DataFrame) -> DataFrame:
    """
    Transform data by building full URLs and deduplicating.
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        DataFrame: Transformed Spark DataFrame
    """
    
    # Build full URLs
    df_with_urls = df.withColumn(
        "page_url", 
        build_full_url_udf(col("trimmed_page_url"), col("site"))
    )
    
    # Filter out null URLs and deduplicate
    df_clean = df_with_urls \
        .filter(col("page_url").isNotNull()) \
        .dropDuplicates(["page_url", "client_id"])
    
    print(f"[INFO] Transformed and deduplicated to {df_clean.count()} rows")
    return df_clean