import unittest
import pandas as pd
import datetime
from functions.helper_functions import combine_transform_data


class TestCombineTransformData(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        self.columns_to_keep = ["id", "value", "location", "inserted_at", "new_id"]
        self.location_name = "TestLocation"
        self.end_combined_df = pd.DataFrame(
            {
                "id": [4, 5],
                "value": ["d", "e"],
                "location": ["OtherLocation", "OtherLocation"],
                "inserted_at": [datetime.datetime.now(), datetime.datetime.now()],
                "new_id": [12345, 67890],
            }
        )

    def test_combine_transform_data_with_non_empty_df(self):
        result_df = combine_transform_data(
            self.location_name, self.df, self.columns_to_keep, self.end_combined_df
        )

        # Check if the result is a DataFrame
        self.assertIsInstance(result_df, pd.DataFrame)

        # Check if the columns are as expected
        self.assertListEqual(list(result_df.columns), self.columns_to_keep)

        # Check if the location column is correctly added
        self.assertTrue((result_df["location"] == self.location_name).any())

        # Check if the inserted_at column is added
        self.assertTrue("inserted_at" in result_df.columns)

        # Check if the new_id column is added
        self.assertTrue("new_id" in result_df.columns)

        # Check if the data is concatenated correctly
        self.assertEqual(len(result_df), len(self.df) + len(self.end_combined_df))

    def test_combine_transform_data_with_empty_end_combined_df(self):
        result_df = combine_transform_data(
            self.location_name, self.df, self.columns_to_keep, None
        )

        # Check if the result is a DataFrame
        self.assertIsInstance(result_df, pd.DataFrame)

        # Check if the columns are as expected
        self.assertListEqual(
            list(result_df.columns),
            self.df.columns.tolist() + ["location", "inserted_at", "new_id"],
        )

        # Check if the location column is correctly added
        self.assertTrue((result_df["location"] == self.location_name).all())

        # Check if the inserted_at column is added
        self.assertTrue("inserted_at" in result_df.columns)

        # Check if the new_id column is added
        self.assertTrue("new_id" in result_df.columns)

        # Check if the data is not concatenated
        self.assertEqual(len(result_df), len(self.df))

    def test_combine_transform_data_with_empty_df(self):
        result = combine_transform_data(
            self.location_name, None, self.columns_to_keep, self.end_combined_df
        )

        # Check if the function returns the correct message
        self.assertEqual(
            result, f"Empty response received for location: {self.location_name}.\n"
        )


if __name__ == "__main__":
    unittest.main()
