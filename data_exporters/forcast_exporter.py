from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
import pandas as pd
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(weather_df, city_dummy, data, batch_id, **kwargs) -> None:
    forecast_df = DataFrame(data['main'])
    city = data['city']

    forecast_df['population'] = city['population']
    forecast_df['sunrise'] = city['sunrise']
    forecast_df['sunset'] = city['sunset']

    loader_df = pd.merge(forecast_df, weather_df, on='dt', how='inner')

    loader_df = loader_df[['city_id', 'date_key', 'population', 'sunrise', 'sunset', 'temp', 'feels_like', 'temp_min',
                           'temp_max', 'pressure', 'sea_level', 'grnd_level', 'humidity', 'temp_kf', 'wind_speed',
                           'wind_deg',
                           'wind_gust', 'clouds_all', 'visibility', 'rain_1h', 'rain_2h', 'rain_3h', 'snow_1h',
                           'snow_2h',
                           'snow_3h', 'weather_combination_id']]
    loader_df['batch_id'] = batch_id

    schema_name = 'public'  # Specify the name of the schema to export data to
    table_name = 'fact_forecast_staging'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    sql_query = f"""
    update fact_forecast set status='INACTIVE' 
    where (city_id, date_key) 
    IN (select city_id, date_key from fact_forecast_staging where batch_id = {batch_id} and status='ACTIVE');
    INSERT INTO public.fact_forecast
    (forecast_id, city_id, date_key, population, sunrise, sunset, "temp", feels_like, 
    temp_min, temp_max, pressure, sea_level, grnd_level, 
    humidity, temp_kf, wind_speed, wind_deg, wind_gust, clouds_all, 
    visibility, rain_1h, rain_2h, rain_3h, snow_1h, snow_2h, snow_3h, weather_combination_id, batch_id, status)
    SELECT forecast_id, city_id, date_key, population, sunrise, sunset, 
    "temp", feels_like, temp_min, temp_max, pressure, sea_level, grnd_level, 
    humidity, temp_kf, wind_speed, wind_deg, wind_gust, clouds_all, visibility, 
    rain_1h, rain_2h, rain_3h, snow_1h, snow_2h, snow_3h, weather_combination_id, batch_id, status
    FROM public.fact_forecast_staging where batch_id={batch_id};
    delete from public.fact_forecast_staging where batch_id={batch_id};
        """

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            loader_df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            allow_reserved_words=True,
            if_exists='APPEND'  # Specify resolution policy if table name already exists
        )
        loader.execute(sql_query)
        loader.commit()
