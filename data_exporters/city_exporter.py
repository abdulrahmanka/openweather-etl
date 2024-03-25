from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(batch_id: int, transformed_data: tuple, **kwargs) -> None:
    city_json = transformed_data['city']
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    finder_query = "select 1 from dim_city where city_id = {city_id}".format(city_id=city_json['id'])
    insert_query = """ 
        INSERT INTO public.dim_city
        (city_id, "name", country, latitude, longitude, timezone,batch_id, status)
        VALUES({city_id}, '{name}', '{country}', {lat}, {lon}, {timezone}, {batch_id}, 'ACTIVE'::character varying)
    """

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        df = loader.load(finder_query)
        if len(df) == 0:
            # insert into dim_city
            loader.execute(insert_query.format(city_id=city_json['id'],
                                               name=city_json['name'],
                                               country=city_json['country'],
                                               lat=city_json['coord']['lat'],
                                               lon=city_json['coord']['lon'],
                                               batch_id=batch_id,
                                               timezone=city_json['timezone']
                                               ))
            loader.commit()
        elif len(df) == 1:
            print("\nNo update needed")
