if 'extension' not in globals():
    from mage_ai.data_preparation.decorators import extension


@extension('great_expectations')
def validate(validator, *args, **kwargs):
    """
    validator: Great Expectations validator object
    """
    validator.expect_column_values_to_be_valid_json()
