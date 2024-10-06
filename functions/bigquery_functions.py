import pandas_gbq
from functions.bigquery_client import bigquery_client
from functions.logger import get_logger
import pandas as pd

logger = get_logger("bigquery-functions")

def push_data_to_bigquery(df, project_id, dataset_id, table_name, if_exists="append"):
    """
    placeholder
    """
    # Check if the response text is not empty

    df = pd.DataFrame(df)
    try:
        check_bq_schema(project_id, dataset_id, table_name, df) # TODO 
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
    """ """
    logger.info(
        f"Checking BigQuery schema for table: {project_id}.{dataset_id}.{table_name}"
    )
    df = pd.DataFrame(df)
    client = bigquery_client()
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = client.get_table(table_id)
    bq_schema = {field.name: field.field_type for field in table.schema}

    df_schema = {col: str(dtype) for col, dtype in df.dtypes.items()}

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




# def validate_schema(df, expected_schema):
#     """
#     Validate the schema of the DataFrame against the expected schema.

#     :param df: DataFrame to validate
#     :param expected_schema: Dictionary with column names as keys and expected data types as values
#     :raises: ValueError if schema does not match
#     """
#     # Check if all expected columns are in the DataFrame
#     for column, expected_dtype in expected_schema.items():
#         if column not in df.columns:
#             raise ValueError(f"Missing expected column: {column}")

#         # Normalize the actual and expected data type strings for comparison
#         actual_dtype = normalize_dtype(str(df[column].dtype))
#         normalized_expected_dtype = normalize_dtype(expected_dtype)

#         if actual_dtype != normalized_expected_dtype:
#             raise ValueError(
#                 f"Column '{column}' has type '{actual_dtype}' but expected '{normalized_expected_dtype}'."
#             )

#     print("Schema validation passed.")
