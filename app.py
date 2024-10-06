import pandas as pd
from functions.helper_functions import (
    get_coordinates,
    get_total_n_earthquakes,
    extract_data_return_df,
    combine_transform_data,
)
from functions.bigquery_functions import push_data_to_bigquery
from functions.logger import get_logger

logger = get_logger("main-script")

# Addresses used for finding the earthquakes
locations = {
    "pleo_dk": "Sortedam Dossering 7 - 4th floor  2200 Copenhagen N",
    "pleo_uk": "Techspace Shoreditch South, Pleo, 32-38 Scrutton street, Buzzer 20, 1st floor, rear unit, EC2A 4RQ",
    "pleo_de": "Karl-Marx-Allee 3, 10178 Berlin",
    "pleo_es": "Calle Gran Via, 39 6th floor 28013 Madrid",
    "pleo_pt": "DP11, Rua Duque de Palmela, 11 1250-096 Lisbon",
    "pleo_ca": "4 Place Ville Marie, 2e+3e étage",
    "pleo_se": "Kungsgatan 49, 111 22",
}

# Define constraints
file_format = "csv"
start_time = "2020-01-01"  # Date format in ISO8601 (YYY-MM-DD)
end_time = "2023-12-31"
maxradiuskm = 500
limit = 20000

# Define the URL templates
url_template = "https://earthquake.usgs.gov/fdsnws/event/1/query?format={file_format}&starttime={start_time}&endtime={end_time}&latitude={latitude}&longitude={longitude}&maxradiuskm={maxradiuskm}&limit={limit}"
count_earthquakes = "https://earthquake.usgs.gov/fdsnws/event/1/count?starttime={start_time}&endtime={end_time}&latitude={latitude}&longitude={longitude}&maxradiuskm={maxradiuskm}"

# BigQuery parameters
project_id = "project-earthquake-432716"
dataset_raw = "raw_data"
dataset_curated = "curated_data"

# Initialize variables
dic_addresses = {}
combined_df = pd.DataFrame()
columns_to_keep_combined_dataset = [
    "hashed_id",
    "time",
    "latitude",
    "longitude",
    "place",
    "location",
    "inserted_at",
]
total_number_earthquakes = 0

logger.info("Starting the extraction process.")

# Get coordinates for the specified locations
dic_addresses = get_coordinates(locations)
logger.info(f"Total number of locations to extract data: {len(dic_addresses)}.")

for location_name, coordinates in dic_addresses.items():
    latitude = coordinates[0]
    longitude = coordinates[1]

    # Format the URL to verify the number of extractions to be done
    url_counts = count_earthquakes.format(
        file_format=file_format,
        start_time=start_time,
        end_time=end_time,
        latitude=latitude,
        longitude=longitude,
        maxradiuskm=maxradiuskm,
    )

    # Get the total number of earthquakes for the location
    dic_number_earthquakes = get_total_n_earthquakes(
        url=url_counts, location_name=location_name
    )

    if total_number_earthquakes <= 2000:
        logger.info(
            "Total number of earthquakes is less than 2000. Proceed with the extraction."
        )

        # Format the URL to extract data
        url_earthquakes = url_template.format(
            file_format=file_format,
            start_time=start_time,
            end_time=end_time,
            latitude=latitude,
            longitude=longitude,
            maxradiuskm=maxradiuskm,
            limit=limit,
        )

        # Extract raw data from source
        extracted_data = extract_data_return_df(
            url=url_earthquakes, location_name=location_name
        )

        # Load raw data to BigQuery
        push_data_to_bigquery(
            project_id=project_id,
            dataset_id=dataset_raw,
            table_name=location_name,
            df=extracted_data,
        )

        # Transform and combine raw data to curated data
        combined_df = combine_transform_data(
            location_name=location_name,
            df=extracted_data,
            columns_to_keep=columns_to_keep_combined_dataset,
            end_combined_df=combined_df,
        )
    else:
        logger.info(
            "Total number of earthquakes exceeds 2000. Split the extraction in less than 2000 rows."
        )

logger.info(
    "Pushing combined data to BigQuery, containing the altered dataset with the location."
)

# Load curated data to BigQuery
push_data_to_bigquery(
    project_id=project_id,
    dataset_id=dataset_curated,
    table_name="earthquakes",
    df=combined_df,
)

logger.info(f"Total rows extracted: {len(combined_df)}.\nExtraction process finished.")
