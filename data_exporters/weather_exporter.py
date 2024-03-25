from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
import pandas as pd
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(data, batch_id: int, **kwargs) -> DataFrame:
    # return data
    # find unique weather conditions add to db
    weather_df = DataFrame(data['weather'])

    unique_weather_data = weather_df[['id', 'main', 'description']].drop_duplicates()
    unique_weather_data = unique_weather_data.rename(columns={"id": "weather_condition_id", "main": "name"})
    unique_weather_data['batch_id'] = batch_id
    unique_weather_data['status'] = 'ACTIVE'

    schema_name = 'public'
    table_name = 'dim_weather_condition'
    unique_constraints = ['weather_condition_id']  # Columns used to identify unique records
    unique_conflict_method = 'IGNORE'  # Method to resolve conflicts
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            unique_weather_data,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            allow_reserved_words=True,
            if_exists='APPEND',  # Specify resolution policy if table name already exists
            unique_constraints=unique_constraints,
            unique_conflict_method=unique_conflict_method
        )
        loader.commit()

    combined_df = weather_df.groupby(['dt'])['id'].apply(list).reset_index(name='weather_condition_id_list')
    weather_df = pd.merge(weather_df, combined_df, on='dt', how='inner')
    weather_df['weather_combination_id'] = weather_df['weather_condition_id_list'].apply(generate_combination_id)

    weather_combo_df = weather_df[['weather_combination_id', 'id']]
    weather_combo_df = weather_combo_df.rename(columns={'id': 'weather_condition_id'})
    weather_combo_df['batch_id'] = batch_id
    weather_combo_df['status'] = 'ACTIVE'

    table_name = 'dim_weather_combination'
    unique_constraints = ['weather_combination_id', 'weather_condition_id']  # Columns used to identify unique records
    unique_conflict_method = 'IGNORE'  # Method to resolve conflicts

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            weather_combo_df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            allow_reserved_words=True,
            if_exists='APPEND',  # Specify resolution policy if table name already exists
            unique_constraints=unique_constraints,
            unique_conflict_method=unique_conflict_method
        )
        loader.commit()

    return weather_df[['dt', 'weather_combination_id']]


def generate_combination_id(weather_condition_id_list) -> int:
    if (len(weather_condition_id_list) == 1):
        return weather_condition_id_list[0]
    return hash(tuple(weather_condition_id_list))
