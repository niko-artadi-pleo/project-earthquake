import unittest
from unittest.mock import patch, MagicMock
from functions.helper_functions import get_total_n_earthquakes


class GetTotalNEarthquakes(unittest.TestCase):
    """
    Unit tests for helper functions related to earthquake data processing.

    Classes:
        GetTotalNEarthquakes: Contains unit tests for the helper functions.

    Methods:
        test_get_total_n_earthquakes(self, mock_get):
            Tests the get_total_n_earthquakes function by mocking the requests.get response.
            Asserts that the function returns the correct number of earthquakes for a given location.
    """

    @patch("functions.helper_functions.requests.get")
    def test_get_total_n_earthquakes(self, mock_get):
        # Mock the response from requests.get
        mock_response = MagicMock()
        mock_response.text = "5"
        mock_get.return_value = mock_response

        url = "http://example.com"
        location_name = "TestLocation"

        result = get_total_n_earthquakes(url, location_name)

        self.assertEqual(result, {location_name: 5})
        self.assertIn(location_name, result)
        self.assertEqual(result[location_name], 5)


if __name__ == "__main__":
    unittest.main()