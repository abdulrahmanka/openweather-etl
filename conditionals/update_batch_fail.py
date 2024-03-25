from openweather_etl.utils.batch_updater import update_batch

if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(*args, **kwargs) -> bool:
    return True
