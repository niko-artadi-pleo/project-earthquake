import pandas_gbq
from functions.bigquery.bigquery_client import bigquery_client
from functions.logger import get_logger
import pandas as pd

logger = get_logger("bigquery-functions")


def push_data_to_bigquery(df, project_id, dataset_id, table_name, if_exists="append"):
    """
    Pushes a pandas DataFrame to a BigQuery table.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be pushed.
        project_id (str): The Google Cloud project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_name (str): The name of the BigQuery table.
        if_exists (str, optional): Specifies the behavior when the table already exists.
        Options are 'fail', 'replace', 'append'. Default is 'append'.

    Returns:
        str: A message indicating if the DataFrame was empty.
    """
    df = pd.DataFrame(df)

    # Check if the DataFrame schema matches the BigQuery table schema
    try:
        check_bq_schema(project_id, dataset_id, table_name, df)
    except ValueError as e:
        logger.error(f"Schema validation failed: {e}")
        return

    # Check if the DataFrame is not empty
    if not df.empty:
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        logger.info(f"Sending data to {table_id}")

        # Create a BigQuery client
        client = bigquery_client()

        # Use the client to push data to BigQuery
        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=project_id,
            if_exists=if_exists,
            credentials=client._credentials,
        )
    else:
        return "Empty DataFrame received and moving to next location."


def check_bq_schema(project_id, dataset_id, table_name, df):
    """
    Checks if the schema of a given DataFrame matches the schema of a specified BigQuery table.

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_name (str): The BigQuery table name.
        df (pd.DataFrame): The DataFrame whose schema needs to be checked.

    Raises:
        ValueError: If a column in the DataFrame is not found in the BigQuery table schema.
        ValueError: If a column's data type in the DataFrame does not match the data type in the BigQuery table schema.

    Logs:
        Logs the process of checking the schema and the result of the check.
    """
    logger.info(
        f"Checking BigQuery schema for table: {project_id}.{dataset_id}.{table_name}"
    )

    df = pd.DataFrame(df)
    client = bigquery_client()
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = client.get_table(table_id)
    bq_schema = {field.name: field.field_type for field in table.schema}

    df_schema = {col: str(dtype) for col, dtype in df.dtypes.items()}

    # Compare DataFrame schema with BigQuery table schema
    for col, dtype in df_schema.items():
        if col not in bq_schema:
            raise ValueError(f"Column '{col}' not found in BigQuery table schema.")
        if bq_schema[col].lower() != dtype.lower():
            raise ValueError(
                f"Column '{col}' has type '{dtype}' in DataFrame but type '{bq_schema[col]}' in BigQuery table."
            )

    logger.info("BigQuery schema matches DataFrame schema.")


# # Example expected schema
# expected_schema = {
#     "time": "string",
#     "latitude": "float64",
#     "longitude": "float64",
#     "depth": "float64",
#     "mag": "float64",
#     "magType": "string",
#     "nst": "float64",
#     "gap": "int64",
#     "dmin": "float64",
#     "rms": "float64",
#     "net": "string",
#     "id": "string",
#     "updated": "string",
#     "place": "string",
#     "type": "string",
#     "horizontalError": "float64",
#     "depthError": "float64",
#     "magError": "float64",
#     "magNst": "int64",
#     "status": "string",
#     "locationSource": "string",
#     "magSource": "string",
#     "Location": "string",  # Custom column added during data processing
# }
