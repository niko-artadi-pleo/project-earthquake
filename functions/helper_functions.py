import requests
import datetime
import pandas as pd
import time
import requests
from io import StringIO
from geopy.geocoders import ArcGIS
from functions.logger import get_logger

logger = get_logger("helper-functions")

def get_total_n_earthquakes(url, location_name):
    """
    placeholder TBC
    """
    logger.info(
        f"Getting the total number of earthquakes for location: {location_name}"
    )
    dic_number_earthquakes = {}
    total_number_earthquakes = 0

    response = requests.get(url)

    total_number_earthquakes += int(response.text)

    dic_number_earthquakes[location_name] = [response.text]

    if total_number_earthquakes <= 2000:
        logger.info(
            f"{total_number_earthquakes} rows to be extracted from {location_name}."
        )
    else:
        logger.info(
            "Total number of earthquakes exceeds 2000. Split the extraction in less than 2000 rows."
        )

    return dic_number_earthquakes


def get_coordinates(locations):
    """ 
    
    """
    logger.info("Getting the geographical coordinates of the locations.")
    # Create an instance of the ArcGIS geocoder
    nom = ArcGIS()
    dic_addresses = {}
    # Loop through each location in the dictionary
    for location_name, address in locations.items():

        # Get the geographical coordinates of the location
        locations = nom.geocode(address)
        if locations:
            # Store the location name with its latitude and longitude
            dic_addresses[location_name] = [
                locations[0],
                locations[1],
            ]

    return dic_addresses


def combine_transform_data(location_name, df, columns_to_keep, end_combined_df):
    """
    
    """
    logger.info(f"Combining and transforming data for location: {location_name}")

    if df is not None:  # Ensure response is not empty or whitespace
        # Add a column for the location name
        df["location"] = location_name

        df["inserted_at"] = datetime.datetime.now()

        # Create a hash column
        df["hashed_id"] = df.apply(
            lambda x: hash(tuple(x[col] for col in df.columns if col != "id")),
            axis=1,
        )
        df.drop_duplicates(subset=["id"])

        # Append the data to the combined DataFrame
        if end_combined_df is None:
            new_df = df

            return new_df
        else:
            combined_df = pd.concat([df, end_combined_df], ignore_index=True)

            return combined_df[columns_to_keep]

    else:
        return f"Empty response received for location: {location_name}.\n"


def extract_data_return_df(url, location_name):
    """
    placeholder
    """

    try:
        logger.info(f"Extracting data for location: {location_name}")
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors

        logger.info(
            f"Sleeping for 1 second."
        )  # TODO update this to integrate time.sleep()
        time.sleep(1)

    except requests.HTTPError as ex:
        logger.error(f"HTTP error occurred for location {location_name}: {ex}")
    except requests.Timeout:
        logger.error(f"Request timed out for location {location_name}.")
    except requests.RequestException as ex:
        logger.error(f"Request exception occurred for location {location_name}: {ex}")
    return pd.read_csv(StringIO(response.text))
