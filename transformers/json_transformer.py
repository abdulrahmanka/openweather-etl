import pandas as pd
import datetime
import math


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
   
    #normalize
    normalized_df = normalize_raw_df(data)
    
    weather_df = nomalize_weather_conditions(normalized_df)

    city_dict = transform_city(data)

    return {"main"      :   normalized_df, 
            "weather"   :   weather_df,
            "city"      :   city_dict
            }


def transform_city(data):
    city_dict = data['city']
    city_dict['sunrise'] = datetime.datetime.fromtimestamp(city_dict['sunrise'])
    city_dict['sunset'] = datetime.datetime.fromtimestamp(city_dict['sunset'])
    return city_dict
    

def nomalize_weather_conditions(forecast_df):
    weather_df = forecast_df[['dt','city_id', 'weather']]
    weather_df = weather_df.explode('weather')

    weather_norm_series = pd.json_normalize(weather_df['weather'])
    weather_df = pd.concat([weather_df.drop('weather', axis=1), weather_norm_series], axis=1)

    return weather_df


def epoch_to_local_str(epoch_timestamp, timezone_offset_seconds):
    """
    Convert an epoch timestamp and a timezone offset in seconds to a local time string.
    
    Parameters:
    - epoch_timestamp: The epoch timestamp to convert.
    - timezone_offset_seconds: The timezone offset from UTC in seconds.
    
    Returns:
    - A string representing the local time in 'YYYYMMDDHH' format.
    """
    # Convert epoch to datetime in UTC
    utc_time = datetime.datetime.utcfromtimestamp(epoch_timestamp)
    
    # # Create a timedelta object for the timezone offset
    # timezone_offset = datetime.timedelta(seconds=timezone_offset_seconds)
    
    # # Calculate local time by applying the timezone offset
    # local_time = utc_time + timezone_offset
    
    # Format and return the local time string
    return utc_time.strftime('%Y%m%d%H')


def normalize_raw_df(data):
    result_df = pd.DataFrame(data['list'])
    
    main_norm_series = pd.json_normalize(result_df['main'])
    #main_norm_series = main_norm_series.rename(columns={c: f'main_{c}' for c in main_norm_series.columns})
    result_df = pd.concat([result_df.drop('main', axis=1), main_norm_series], axis=1)

    wind_norm_series = pd.json_normalize(result_df['wind'])
    wind_norm_series = wind_norm_series.rename(columns={c: f'wind_{c}' for c in wind_norm_series.columns})
    result_df = pd.concat([result_df.drop('wind', axis=1), wind_norm_series], axis=1)

    rain_norm_series = pd.json_normalize(result_df['rain'])
    rain_norm_series = rain_norm_series.rename(columns={c: f'rain_{c}' for c in rain_norm_series.columns})
    result_df = pd.concat([result_df.drop('rain', axis=1), rain_norm_series], axis=1)

    clouds_norm_series = pd.json_normalize(result_df['clouds'])
    clouds_norm_series = clouds_norm_series.rename(columns={c: f'clouds_{c}' for c in clouds_norm_series.columns})
    result_df = pd.concat([result_df.drop('clouds', axis=1), clouds_norm_series], axis=1)

    timezone = data['city']['timezone']

    result_df['date_key'] = result_df['dt'].apply(lambda x: epoch_to_local_str(x, timezone))
    result_df['city_id'] = data['city']['id']
    result_df['visibility'] = result_df['visibility'].apply(lambda x: 0 if math.isnan(x) else int(x))
    return result_df




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
