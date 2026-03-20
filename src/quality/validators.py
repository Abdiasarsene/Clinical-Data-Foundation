# src/quality/validators.py
from src.quality.context import get_ge_context
from src.quality.exception import DataQualityFailure

# ====== VALIDATION ======
def validate_table(
    datasource_name: str,
    asset_name: str,
    suite_name: str
):
    context = get_ge_context()

    # Securing the table name: whitelist or strict validation
    if not asset_name.isidentifier():
        raise DataQualityFailure(f"Invalid asset name: {asset_name}")
    query = "SELECT * FROM {}".format(asset_name)

    batch_request = {
        "datasource_name": datasource_name,
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": asset_name,
        "runtime_parameters": {"query": query},
        "batch_identifiers": {"default_identifier_name": "default"},
    }

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    results = validator.validate()
    if not results["success"]:
        raise DataQualityFailure(
            f"Validation failed for {asset_name}",
            asset=asset_name
        )
    return results