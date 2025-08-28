# URL Content Classification Pipeline

## Overview

This project implements a scalable pipeline for classifying web impression URLs using Google Cloud, Spark, and Airflow. It extracts URLs from BigQuery, fetches page content, applies NLP categorization, and loads results back to BigQuery.

## Architecture

- **Orchestration:** Airflow DAG manages the workflow.
- **Processing:** Spark handles extraction and transformation.
- **Content Fetching:** aiohttp, requests, and Playwright retrieve HTML.
- **NLP Categorization:** Google Cloud Natural Language API calls to classify content.
- **Storage:** BigQuery for input and output data.

## Data Flow

1. **Extract:** Pull URLs from GA4 BigQuery table.
2. **Transform:** Clean and deduplicate URLs.
3. **Filter:** Remove URLs already processed.
4. **Batch:** Stage uncached URLs for parallel processing.
5. **Fetch:** Download page content for each URL.
6. **Categorize:** Apply NLP and map categories.
7. **Load:** Write results to BigQuery.

## Key Files

- `dags/url_content_topics/dag_url_content_batch.py`: Airflow DAG
- `src/extract.py`: Extraction logic
- `src/transform.py`: Transformation logic
- `utils/web_fetch.py`: Content fetching
- `src/categorize.py`: NLP categorization
- `src/load.py`: BigQuery loading

## Configuration

- Project/table settings: `config/project_config.py`
- Airflow variables: GCS temp bucket, skip reconcile flag

## Deployment

- Cloud Build deploys DAGs and code to GCS ([cloudbuild/deploy.yaml](cloudbuild/deploy.yaml))
- Requirements: See [requirements.txt](requirements.txt)

## Extending or Debugging

- Add new transformation logic in [`src/transform.py`](src/transform.py)
- Update category mapping in [`utils/category_mapping.py`](utils/category_mapping.py)
- Modify DAG parameters in [`dags/url_content_topics/dag_url_content_batch.py`](dags/url_content_topics/dag_url_content_batch.py)

## References

- [Google Cloud Natural Language API](https://cloud.google.com/natural-language)
