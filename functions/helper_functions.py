import requests
import datetime
import pandas as pd
import time
from io import StringIO
from geopy.geocoders import ArcGIS
from functions.logger import get_logger

logger = get_logger("helper-functions")


import requests
import logging

logger = logging.getLogger("helper-functions")

def get_total_n_earthquakes(url, location_name):
    """
    Gets the total number of earthquakes for a given location.

    Args:
        url (str): The URL to fetch the data from.
        location_name (str): The name of the location.

    Returns:
        dict: A dictionary with the location name as the key and the total number of earthquakes as the value.
    """
    logger.info(f"Getting the total number of earthquakes for location: {location_name}")
    response = requests.get(url)
    response.raise_for_status()
    total_earthquakes = int(response.text.strip())
    logger.info(f"{total_earthquakes} rows to be extracted from {location_name}.")
    return {location_name: total_earthquakes}


def get_coordinates(locations):
    """
    Get the geographical coordinates of the given locations.
    This function takes a dictionary of location names and their addresses,
    and returns a dictionary with the location names as keys and their
    corresponding latitude and longitude as values.

    Args:
        locations (dict): A dictionary where keys are location names and
        values are addresses.

    Returns:
        dict: A dictionary where keys are location names and values are
        lists containing latitude and longitude.
    """
    logger.info("Getting the geographical coordinates of the locations.")
    nom = ArcGIS()  # Create an instance of the ArcGIS geocoder
    dic_addresses = {}

    for location_name, address in locations.items():
        coordinates = nom.geocode(address)
        if locations:
            dic_addresses[location_name] = [
                coordinates.latitude, 
                coordinates.longitude]

    return dic_addresses


def combine_transform_data(location_name, df, columns_to_keep, end_combined_df):
    """
    Combines and transforms data for a given location.

    This function adds a location name and a timestamp to the DataFrame, creates a hashed ID for each row,
    removes duplicates based on the 'id' column, and appends the data to an existing combined DataFrame.

    Args:
        location_name (str): The name of the location to be added to the DataFrame.
        df (pd.DataFrame): The DataFrame containing the data to be transformed.
        columns_to_keep (list): A list of column names to keep in the final combined DataFrame.
        end_combined_df (pd.DataFrame or None): The existing combined DataFrame to which the new data will be appended.
                                                If None, a new DataFrame will be created.

    Returns:
        pd.DataFrame: The combined and transformed DataFrame.
        str: A message indicating an empty response if the input DataFrame is None.
    """
    logger.info(f"Combining and transforming data for location: {location_name}")

    if df is not None:
        df["location"] = location_name  # Add a column for the location name
        df["inserted_at"] = datetime.datetime.now()  # Add a timestamp

        # Create a hash column
        df["hashed_id"] = df.apply(
            lambda x: hash(tuple(x[col] for col in df.columns if col != "id")),
            axis=1,
        )
        df.drop_duplicates(subset=["id"])  # Remove duplicates based on 'id' column

        if end_combined_df is None:
            return df
        else:
            combined_df = pd.concat([df, end_combined_df], ignore_index=True)
            return combined_df[columns_to_keep]
    else:
        return f"Empty response received for location: {location_name}.\n"


def extract_data_return_df(url, location_name):
    """
    Extracts data from a given URL and returns it as a pandas DataFrame.

    Args:
        url (str): The URL from which to extract data.
        location_name (str): The name of the location for logging purposes.

    Returns:
        pandas.DataFrame: The extracted data as a DataFrame.

    Raises:
        requests.HTTPError: If an HTTP error occurs.
        requests.Timeout: If the request times out.
        requests.RequestException: If a general request exception occurs.

    Logs:
        Logs the start of data extraction, any HTTP errors, timeouts, or other request exceptions.
    """
    try:
        logger.info(f"Extracting data for location: {location_name}")
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors

        logger.info("Sleeping for 1 second.")
        time.sleep(1)  # Sleep for 1 second to avoid hitting rate limits

        return pd.read_csv(StringIO(response.text))

    except requests.HTTPError as ex:
        logger.error(f"HTTP error occurred for location {location_name}: {ex}")
        raise
    except requests.Timeout as ex:
        logger.error(f"Request timed out for location {location_name}: {ex}")
        raise
    except requests.RequestException as ex:
        logger.error(f"Request exception occurred for location {location_name}: {ex}")
        raise