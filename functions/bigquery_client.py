from google.cloud import bigquery
import os
from functions.logger import get_logger

logger = get_logger("bigquery-client")


def bigquery_client():
    # Path to the service account key file
    key_path = "/Users/nikolas.artadi/Documents/personal/Project Earthquake/bigquery-project-earthquake-secrets.json"
    logger.info("Create  BigQuery client.")
    # Set the environment variable for authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    # Create a BigQuery client
    client = bigquery.Client()

    return client

# Example usage
if __name__ == "__main__":
    client = bigquery_client()
    logger.info("BigQuery client created successfully.")
