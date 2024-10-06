import unittest
from unittest.mock import patch, MagicMock
from functions.helper_functions import get_total_n_earthquakes, get_coordinates


class TestHelperFunctions(unittest.TestCase):

    @patch("helper_functions.requests.get")
    def test_get_total_n_earthquakes(self, mock_get):
        # Mock the response from requests.get
        mock_response = MagicMock()
        mock_response.text = "5"
        mock_get.return_value = mock_response

        url = "http://example.com"
        location_name = "TestLocation"

        result = get_total_n_earthquakes(url, location_name)

        self.assertEqual(result, {location_name: ["5"]})
        self.assertIn(location_name, result)
        self.assertEqual(result[location_name], ["5"])

    @patch("helper_functions.ArcGIS")
    def test_get_coordinates(self, mock_arcgis):
        # Mock the geocode method
        mock_geocoder = MagicMock()
        mock_geocoder.geocode.return_value = MagicMock(
            latitude=40.7128, longitude=-74.0060
        )
        mock_arcgis.return_value = mock_geocoder

        locations_dict = {"New York": "New York, NY"}

        result = get_coordinates(locations_dict)

        self.assertEqual(result, {"New York": [40.7128, -74.0060]})
        self.assertIn("New York", result)
        self.assertEqual(result["New York"], [40.7128, -74.0060])


if __name__ == "__main__":
    unittest.main()
