import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from functions.helper_functions import combine_transform_data


class CombineTransformData(unittest.TestCase):
    """
    Unit tests for the combine_transform_data function.

    CombineTransformData is a test case class that contains unit tests for the
    combine_transform_data function. The tests ensure that the function correctly
    combines and transforms data from multiple DataFrames.

    Methods
    -------
    setUp():
        Sets up the sample data and columns to keep for testing.

    test_combine_transform_data_with_empty_end_combined_df():
        Tests the combine_transform_data function when the end_combined_df is empty.

    test_combine_transform_data_with_non_empty_end_combined_df():
        Tests the combine_transform_data function when the end_combined_df is not empty.

    test_combine_transform_data_with_empty_df():
        Tests the combine_transform_data function when the input DataFrame is empty.
    """

    def setUp(self):
        # Sample data for testing
        self.df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        self.df2 = pd.DataFrame({"id": [4, 5, 6], "value": ["d", "e", "f"]})
        self.columns_to_keep = ["id", "value", "location", "inserted_at"]

    def test_combine_transform_data_with_empty_end_combined_df(self):
        result = combine_transform_data(
            "Location1", self.df1, self.columns_to_keep, None
        )
        self.df1["location"] = "Location1"
        self.df1["inserted_at"] = result["inserted_at"]
        assert_frame_equal(result[self.columns_to_keep], self.df1[self.columns_to_keep])

    def test_combine_transform_data_with_non_empty_end_combined_df(self):
        self.df1["location"] = "Location1"
        self.df2["location"] = "Location2"
        combined_df = pd.concat([self.df2, self.df1], ignore_index=True)
        result = combine_transform_data(
            "Location1", self.df1, self.columns_to_keep, self.df2
        )

        # Ensure 'inserted_at' values are correctly assigned
        combined_df["inserted_at"] = combined_df.apply(
            lambda x: result["inserted_at"][result["id"] == x["id"]].values[0], axis=1
        )

        # Convert 'inserted_at' to the same dtype
        result["inserted_at"] = result["inserted_at"].astype("datetime64[ns]")
        combined_df["inserted_at"] = combined_df["inserted_at"].astype("datetime64[ns]")

        # Sort both DataFrames by 'id' before comparing
        result_sorted = (
            result[self.columns_to_keep].sort_values(by="id").reset_index(drop=True)
        )
        combined_df_sorted = (
            combined_df[self.columns_to_keep]
            .sort_values(by="id")
            .reset_index(drop=True)
        )
        assert_frame_equal(result_sorted, combined_df_sorted)

    def test_combine_transform_data_with_empty_df(self):
        result = combine_transform_data(
            "Location3", None, self.columns_to_keep, self.df1
        )
        self.assertEqual(result, "Empty response received for location: Location3.\n")


if __name__ == "__main__":
    unittest.main()
