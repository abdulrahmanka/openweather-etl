if 'callback' not in globals():
    from mage_ai.data_preparation.decorators import callback


@callback('success')
def success_callback(parent_block_data, **kwargs):
    pass

from openweather_etl.utils.batch_updater import update_batch

@callback('failure')
def failure_callback(parent_block_data, **kwargs):
     pass
    #  update_batch(batch_id, 'FAILED')
