from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType, FloatType, BooleanType, StructType, StructField
from config.project_config import PROJECT_ID
from google.cloud import language_v1
from bs4 import BeautifulSoup
import requests
import time

def classify_text(text):
    """ Classify text using Google Cloud Natural Language API."""

    try:
        client = language_v1.LanguageServiceClient(
            client_options={"quota_project_id": PROJECT_ID}
        )
        document = language_v1.Document(
            content=text,
            type_=language_v1.Document.Type.PLAIN_TEXT
        )
        response = client.classify_text(
            request={
                "document": document,
                "classification_model_options": {"v2_model": {}}
            }
        )
        return response.categories
    except Exception as e:
        print(f"[ERROR] NLP classification failed: {e}")
        return []
    

def fetch_page_text(url):
    """ Fetch and extract text from a webpage."""

    try:
        response = requests.get(url, timeout=5)
        print(f"[DEBUG] URL fetched: {url} - Status: {response.status_code}")
        if response.status_code != 200:
            return ""
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(separator=' ', strip=True)
        print(f"[DEBUG] Word count: {len(text.split())}")
        return text
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
        return ""

def categorize_single_url(url):
    """
    Categorize a single URL - returns tuple of (topic, confidence, review_flag)
    This function will be used as a UDF
    """
    # Add rate limiting to avoid API overload
    time.sleep(1) 
    
    text = fetch_page_text(url)
    if len(text.split()) < 20:
        return (None, None, True)
    
    try:
        categories = classify_text(text)
        print(f"[DEBUG] Categories returned for {url}: {categories}")
        if categories:
            top_cat = categories[0]
            return (top_cat.name, float(top_cat.confidence), top_cat.confidence < 0.6)
        else:
            return (None, None, True)
    except Exception as e:
        print(f"[ERROR] NLP failed for {url}: {e}")
        return (None, None, True)

# Define return type for UDF
categorize_schema = StructType([
    StructField("content_topic", StringType(), True),
    StructField("prediction_confidence", FloatType(), True),
    StructField("review_flag", BooleanType(), True)
])

# Register UDF
categorize_udf = udf(categorize_single_url, categorize_schema)

def categorize_urls(df: DataFrame) -> DataFrame:
    """
    Categorize URLs using Google NLP API.
    
    WARNING: This is the bottleneck for large datasets due to sequential API calls.
    Consider batching or using a different approach for TB-scale data.
    """
    
    # Apply categorization UDF
    df_with_categories = df.withColumn("categorization", categorize_udf(col("page_url")))
    
    # Extract fields from struct
    df_final = df_with_categories \
        .withColumn("content_topic", col("categorization.content_topic")) \
        .withColumn("prediction_confidence", col("categorization.prediction_confidence")) \
        .withColumn("review_flag", col("categorization.review_flag")) \
        .drop("categorization")
    
    return df_final
