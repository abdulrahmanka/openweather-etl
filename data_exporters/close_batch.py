from openweather_etl.utils.batch_updater import update_batch

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(d1: None,batch_id: int, **kwargs) -> None:
    update_batch(batch_id, 'DONE')



