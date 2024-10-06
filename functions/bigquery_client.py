from google.cloud import bigquery
import os
from functions.logger import get_logger

logger = get_logger("bigquery-client")


def bigquery_client():
    """
    Creates and returns a BigQuery client using a service account key file for authentication.

    The function sets the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the path of the
    service account key file and then creates a BigQuery client.

    Returns:
        google.cloud.bigquery.Client: An authenticated BigQuery client.

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If the credentials are not found or invalid.
    """
    # Path to the service account key file
    key_path = "/Users/nikolas.artadi/Documents/personal/project-earthquake/bigquery-project-earthquake-secrets.json"
    logger.info("Create  BigQuery client.")
    # Set the environment variable for authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    # Create a BigQuery client
    client = bigquery.Client()

    return client
