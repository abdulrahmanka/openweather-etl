from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path


def update_batch(batch_id: int, status: str) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    query = f"""update public.tbl_batches set updated_at = CURRENT_TIMESTAMP, update_user='etl_user',
     status='{status}' where batch_id = {batch_id}"""

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
       loader.execute(query)
       loader.commit()



