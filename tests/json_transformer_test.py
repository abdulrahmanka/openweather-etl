import unittest
import logging
import json
import pandas as pd
from openweather_etl.transformers.json_transformer import normalize_raw_df, normalize_weather_conditions
default_logger = logging.getLogger(__name__)

class JsonTransformerTest(unittest.TestCase):

    with(open("dummy_response.json", "r")) as f:
        data = json.load(f)

    def test_normalize_raw_df(self):
        result_df = normalize_raw_df(self.data)
        expected_df = pd.read_csv("main_normalized.csv")

        assert set(result_df.columns) == set(expected_df.columns), "Columns are different"
        comparison_df = expected_df.compare(result_df)
        assert comparison_df.empty, f"Dataframes are different: {comparison_df}"


    def test_normalize_weather_conditions(self):
        weather_df = normalize_weather_conditions(self.data)
        expected_df = pd.read_csv("weather_normalized.csv")

        assert set(weather_df.columns) == set(expected_df.columns), "Columns are different"
        comparison_df = expected_df.compare(weather_df)
        assert comparison_df.empty, f"Dataframes are different: {comparison_df}"


