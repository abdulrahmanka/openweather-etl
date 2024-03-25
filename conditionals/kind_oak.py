from openweather_etl.utils.batch_updater import update_batch

# if 'callback' not in globals():
#     from mage_ai.data_preparation.decorators import callback
    
@callback('failure')
def only_run_this_function_on_failure(parent_block_data, **kwargs):
    update_batch(batch_id, 'FAILED')