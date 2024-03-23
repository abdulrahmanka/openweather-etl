import io
import pandas as pd
import requests
from typing import Tuple


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs) -> Tuple[str, pd.DataFrame]:
    """
    Template for loading data from API
    """
    url = 'https://api.openweathermap.org/data/2.5/forecast?lat=44.34&lon=10.99&appid=42bfc7e6fb5ac240a9109dd6a71682b3'

    
    response = requests.get(url)

    return response.json()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
