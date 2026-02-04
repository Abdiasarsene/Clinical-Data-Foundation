from quality.context import get_ge_context

def validate_table(
    datasource_name: str,
    asset_name: str,
    suite_name: str
):
    context = get_ge_context()

    batch_request = {
        "datasource_name": datasource_name,
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": asset_name,
        "runtime_parameters": {"query": f"SELECT * FROM {asset_name}"},
        "batch_identifiers": {"default_identifier_name": "default"}
    }

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    return validator.validate()