import unittest
from mage_ai.data_preparation.models.pipeline import Pipeline
from unittest.mock import patch
from mage_ai.data_preparation.shared.secrets import get_secret_value
import requests
import logging
from openweather_etl.data_loaders.data_downloader import load_data_from_api
import responses

default_logger = logging.getLogger(__name__)

class DataDownloaderTest(unittest.TestCase):
    @responses.activate
    @patch('openweather_etl.tests.amazon_cloud_pipeline_test.get_secret_value',
           return_value='mocked_appid')
    def test_data_downloader(self, mocked_get_secret_value):
        pipeline = Pipeline.get('amazing_cloud', repo_path="../")
        data_downloader = pipeline.get_block('data_downloader')
        lat = 12.9716
        lon = 77.5946
        global_vars ={"lat": lat, "lon": lon, "logger": default_logger}
        mock_data = {'key': 'value'}

        url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid=None"

        rsp1 = responses.Response(
            method="GET",
            url=url,
            json=mock_data,
            status=200,
        )
        responses.add(rsp1)

        result = load_data_from_api(None, **global_vars)
        assert result == mock_data

    @responses.activate
    @patch('openweather_etl.tests.amazon_cloud_pipeline_test.get_secret_value',
           return_value='mocked_appid')
    def test_data_downloader(self, mocked_get_secret_value):
        pipeline = Pipeline.get('amazing_cloud', repo_path="../")
        data_downloader = pipeline.get_block('data_downloader')
        lat = 12.9716
        lon = 77.5946
        global_vars = {"lat": lat, "lon": lon, "logger": default_logger}
        mock_data = {'key': 'value'}

        url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid=None"

        rsp1 = responses.Response(
            method="GET",
            url=url,
            json=mock_data,
            status=400,
        )
        responses.add(rsp1)
        with self.assertRaises(Exception):
            load_data_from_api(None, **global_vars)

        with self.assertRaises(requests.ConnectionError):
            global_vars = {"lat": lat, "lon": lat, "logger": default_logger}
            load_data_from_api(None, **global_vars)

        #Add for timeout and Request Exception