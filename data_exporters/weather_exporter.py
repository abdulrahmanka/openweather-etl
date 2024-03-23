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
    print(type(weather_df))

    unique_weather_data = weather_df[['id', 'main', 'description']].drop_duplicates()
    unique_weather_data = unique_weather_data.rename(columns={"id": "weather_condition_id"})
    unique_weather_data = unique_weather_data.rename(columns={"main": "name"})

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
            allow_reserved_words = True,
            if_exists = 'APPEND',  # Specify resolution policy if table name already exists
            unique_constraints = unique_constraints,
            unique_conflict_method = unique_conflict_method
        )
        # combined_weather_df = loader.load("select combined_weather_condition_id, weather_condition_id from dim_combined_weather_condition order by weather_condition_id")


    combined_df = weather_df.groupby(['dt'])['id'].apply(list).reset_index(name='weather_condition_id_list')         
    
    # combined_df = weather_df.groupby('dt')['id'].agg(lambda x: ','.join(sorted(map(str, x)))).reset_index(name='weather_condition_id_list')

    # combined_weather_df = combined_weather_df.groupby('combined_weather_condition_id')['weather_condition_id'].agg(lambda x: ','.join(sorted(map(str, x)))).reset_index(name='weather_condition_id_list')
    
    # combined_df = pd.merge(combined_df, combined_weather_df, on="weather_condition_id_list",how="left")

    #find where combined_weather_condition_id is null and insert new record into 

    # # for a single time stamp find combo weather condition and add combo weather entry and link to row

    # schema_name = 'your_schema_name'  # Specify the name of the schema to export data to
    # table_name = 'dim_combined_weather_condition'  # Specify the name of the table to export data to
    # config_path = path.join(get_repo_path(), 'io_config.yaml')
    # config_profile = 'default'

    # with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
    #     loader.export(
    #         df,
    #         schema_name,
    #         table_name,
    #         index=False,  # Specifies whether to include index in exported table
    #         if_exists='replace',  # Specify resolution policy if table name already exists
    #     )
    return combined_df
