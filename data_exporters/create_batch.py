from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def create_batch(data, **kwargs) -> DataFrame:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    query = """INSERT INTO public.tbl_batches
    ( batch_name, batch_description, status, inserted_at, insert_user)
    VALUES('weather_forcast-etl', '', 'PROCESSING'::character varying, CURRENT_TIMESTAMP, 'elt_user') 
    RETURNING batch_id;"""

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        batch_df = loader.load(query)
        batch_id = batch_df.iloc[0, 0]
        loader.commit()
        
        return batch_id
