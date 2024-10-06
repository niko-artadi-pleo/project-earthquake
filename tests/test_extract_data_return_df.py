import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from io import StringIO
import requests
import time
from functions.helper_functions import extract_data_return_df


class TestHelperFunctions(unittest.TestCase):

    @patch("helper_functions.requests.get")
    @patch("helper_functions.time.sleep", return_value=None)
    def test_extract_data_return_df_success(self, mock_sleep, mock_get):
        # Mock the response from requests.get
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = "col1,col2\nval1,val2\nval3,val4"
        mock_get.return_value = mock_response

        url = "http://example.com"
        location_name = "Test Location"

        # Call the function
        result_df = extract_data_return_df(url, location_name)

        # Create expected DataFrame
        expected_df = pd.read_csv(StringIO(mock_response.text))

        # Assert the DataFrame is as expected
        pd.testing.assert_frame_equal(result_df, expected_df)
        mock_get.assert_called_once_with(url)
        mock_response.raise_for_status.assert_called_once()
        mock_sleep.assert_called_once_with(1)

    @patch("helper_functions.requests.get")
    @patch("helper_functions.time.sleep", return_value=None)
    def test_extract_data_return_df_http_error(self, mock_sleep, mock_get):
        # Mock the response from requests.get to raise an HTTPError
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("HTTP Error")
        mock_get.return_value = mock_response

        url = "http://example.com"
        location_name = "Test Location"

        # Call the function
        with self.assertRaises(requests.HTTPError):
            extract_data_return_df(url, location_name)

        mock_get.assert_called_once_with(url)
        mock_response.raise_for_status.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("helper_functions.requests.get")
    @patch("helper_functions.time.sleep", return_value=None)
    def test_extract_data_return_df_timeout(self, mock_sleep, mock_get):
        # Mock the response from requests.get to raise a Timeout
        mock_get.side_effect = requests.Timeout("Timeout Error")

        url = "http://example.com"
        location_name = "Test Location"

        # Call the function
        with self.assertRaises(requests.Timeout):
            extract_data_return_df(url, location_name)

        mock_get.assert_called_once_with(url)
        mock_sleep.assert_not_called()

    @patch("helper_functions.requests.get")
    @patch("helper_functions.time.sleep", return_value=None)
    def test_extract_data_return_df_request_exception(self, mock_sleep, mock_get):
        # Mock the response from requests.get to raise a RequestException
        mock_get.side_effect = requests.RequestException("Request Exception")

        url = "http://example.com"
        location_name = "Test Location"

        # Call the function
        with self.assertRaises(requests.RequestException):
            extract_data_return_df(url, location_name)

        mock_get.assert_called_once_with(url)
        mock_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
