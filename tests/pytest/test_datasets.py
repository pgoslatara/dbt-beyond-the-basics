import pytest


@pytest.mark.catalog_json
def test_dataset_name(catalog_json: dict) -> None:
    """
    Dataset names should not contain "None". This can occur when the `generate_schema_name` macro is incorrectly edited or a new directory is added to `./models` without a corresponding entry in `./dbt_project.yml`.

    This test needs to run after catalog.json is built (i.e. `dbt docs generate`).
    """

    for k, v in catalog_json["nodes"].items():
        dataset = v["metadata"]["schema"]
        assert (
            "None" not in dataset
        ), f"Dataset name contains 'None' ({dataset}) for node {v['unique_id'].split('.')[-1]}. This can happen when the generate_schema_name macro is mis-configured"
