import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs) -> str:
    """
    Template for loading data from API
    """
    logger = kwargs.get('logger')
    lat = kwargs.get('lat')
    lon = kwargs.get('lon')
    appid = get_secret_value('openweater_appid')
    logger.info(f"Invoking API with lat as {lat} and lon as {lon}")
    
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={appid}"

    
    try:
        response = requests.get(url)


        if response.status_code == 200:
            logger.debug("API Response {}".format(response.json()))
            return response.json()
        # elif response.status_code == 429:
        #     #rate limit exceeded , wait unit next day UTC
        #     set_global_variable("weathered_byte", "RATE_LIMIT_EXCEEDED", True)
        #     set_global_variable("weathered_byte", "RATE_LIMIT_EXCEEDED_DATE", value)

        #     return "RATE_LIMIT_EXCEEDED"
        else:
            logger.error(f"API Request Failed with status code: {response.status_code}, Message: {response.json()}")
            raise Exception(f"API Request Failed with status code: {response.status_code}, Message: {response.json()}")
    except requests.ConnectionError:
        logger.exception("API Connection Error. The URL may be down or incorrect.")
        raise
    except requests.Timeout:
        logger.exception("API Request timed out.")
        raise
    except requests.RequestException as e:
        logger.exception(f"An error occurred while making the API request: {e}")
        raise
    except Exception as e:
        logger.exception(f"An unspecified error occurred: {e}")
        raise



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


@test
def test_schema(output, *args) -> None:
    # load file contents of response_schema.json
    with open('openweather_etl/data_loaders/response_schema.json') as f:
        schema_ow = json.load(f)
    validate_json(output, schema_ow)
    assert validate_json(output, schema_ow) is True, 'Output is valid'

def validate_json(data, schema):
    try:
        validate(instance=data, schema=schema)
        return True
    except ValidationError as e:
        print(f"JSON data is invalid: {e.message}")
        raise

