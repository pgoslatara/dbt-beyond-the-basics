import argparse
import json
import logging
from pathlib import Path
from typing import List

from utils import download_manifest_json, get_gcp_auth_clients, set_logging_options


def drop_cicd_datasets(environment: str, dataset_pattern: str) -> None:
    """DROP datasets that that start with a specified pattern."""

    # Determine relevant datasets/schemas
    client = get_gcp_auth_clients(environment)["bigquery"]
    query = f"""
        SELECT
            schema_name
        FROM `{client.project}.INFORMATION_SCHEMA.SCHEMATA`
        WHERE
            schema_name LIKE "{dataset_pattern}%"
    """
    query_job = client.query(query)
    filtered_datasets = [row.schema_name for row in query_job]
    logging.info(f"Found {len(filtered_datasets)} matching datasets.")

    # Delete datasets
    for dataset in filtered_datasets:
        logging.info(f"Deleting {client.project}.{dataset}...")
        client.delete_dataset(
            f"{client.project}.{dataset}", delete_contents=True, not_found_ok=True
        )


def drop_empty_datasets(environment: str) -> None:
    """DROP datasets that are empty."""

    client = get_gcp_auth_clients(environment)["bigquery"]
    for dataset_id in list(client.list_datasets()):
        if (
            len(list(client.list_routines(dataset_id))) == 0
            and len(list(client.list_tables(dataset_id))) == 0
        ):
            logging.info(
                f"DROPping dataset {dataset_id.dataset_id} as it contains no routines or tables..."
            )
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def drop_orphaned_dbt_tables(environment: str) -> None:
    """DROP all tables that contain the "dbt" labek but are not in the manifest.json."""

    client = get_gcp_auth_clients(environment)["bigquery"]
    download_manifest_json(
        env=environment,
        destination_file_name="./.state/manifest.json",
        version="latest",
    )

    with Path("./.state/manifest.json").open() as f:
        manifest_json = json.load(f)

    latest_dbt_tables = [
        f"{v['schema']}.{v['name']}"
        for k, v in manifest_json["nodes"].items()
        if v["resource_type"] == "model"
    ]
    logging.info(f"Found {len(latest_dbt_tables)} tables in latest manifest.json...")

    logging.info("Searching for tables with tag 'created_by' == 'dbt'...")
    bq_tables_with_dbt_label: List[str] = []
    client = get_gcp_auth_clients(environment)["bigquery"]
    for dataset_id in list(client.list_datasets()):
        for table_id in client.list_tables(dataset_id):
            table = client.get_table(table_id)
            if table.labels:
                bq_tables_with_dbt_label.extend(
                    table_id
                    for label, value in table.labels.items()
                    if label == "created_by" and value == "dbt"
                )
    logging.info(
        f"Found {len(bq_tables_with_dbt_label)} tables with tag 'created_by' == 'dbt'..."
    )

    orphaned_dbt_tables = [
        x
        for x in bq_tables_with_dbt_label
        if f"{x.dataset_id}.{x.table_id}" not in latest_dbt_tables
    ]
    logging.info(f"Found {len(orphaned_dbt_tables)} orphaned tables...")

    for i in orphaned_dbt_tables:
        logging.info(f"DROPping table {i.project}.{i.dataset_id}.{i.table_id}...")
        client.delete_table(
            f"{i.project}.{i.dataset_id}.{i.table_id}", not_found_ok=True
        )


def main() -> None:
    set_logging_options()

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset_pattern", help="Pattern of datasets to DROP.", required=True
    )
    parser.add_argument("--environment", help="The environment to use.", required=True)
    args = parser.parse_args()

    assert args.environment in [
        "prd",
        "stg",
    ], "Only prd and stg are valid inputs to `environment`."

    drop_cicd_datasets(
        environment=args.environment, dataset_pattern=args.dataset_pattern
    )
    drop_orphaned_dbt_tables(environment=args.environment)
    drop_empty_datasets(environment=args.environment)


if __name__ == "__main__":
    main()
