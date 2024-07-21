import argparse
import json
import logging
import os
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import List, Mapping

import duckdb
import pyarrow as pa
import pytablewriter
import yaml
from google.api_core.exceptions import BadRequest, NotFound
from jinja2 import Template
from retry import retry
from utils import (
    ManifestInitRunError,
    delete_github_pr_bot_comments,
    download_manifest_json,
    get_gcp_auth_clients,
    run_dbt_command,
    send_github_pr_comment,
    set_logging_options,
)


def parse_command_line_args() -> tuple:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dbt_dataset",
        help="Dataset where the CI tables are located",
        required=True,
    )
    parser.add_argument(
        "--pull_request_id",
        help="ID of the pull request",
        required=True,
    )
    parser.add_argument(
        "--target_branch",
        help="The target branch of the pull request",
        required=True,
        type=str,
    )
    args = parser.parse_args()

    dbt_dataset = args.dbt_dataset
    pull_request_id = args.pull_request_id
    target_branch = args.target_branch

    logging.info(f"{dbt_dataset=}")
    logging.info(f"{pull_request_id=}")
    logging.info(f"{target_branch=}")

    return (dbt_dataset, pull_request_id, target_branch)


def compare_manifests_and_comment_impacted_models(
    env: str, manifest_file_name: str, pull_request_id: int
) -> None:
    """Download the latest manifest for the env, compare to current manifest.json, add a comment to the PR with impacted models"""

    download_manifest_json(
        env=env, destination_file_name=manifest_file_name, version="latest"
    )

    directly_impacted_models = sorted(
        run_dbt_command(
            f"dbt --quiet ls --select state:modified --state ./.state --resource-type model --target {env}"
        )
    )
    logging.info(f"{directly_impacted_models=}")
    direct_md = "\n".join([f'| {x.split(".")[-1]} |' for x in directly_impacted_models])

    indirectly_impacted_models = sorted(
        run_dbt_command(
            f"dbt --quiet ls --select state:modified+ --exclude state:modified --state ./.state --resource-type model --target {env}"
        )
    )
    logging.info(f"{indirectly_impacted_models=}")
    indirect_md = "\n".join(
        [f'| {x.split(".")[-1]} |' for x in indirectly_impacted_models]
    )

    with Path("./target/manifest.json").open() as f:
        manifest_json = json.load(f)

    impacted_exposures = sorted(
        run_dbt_command(
            f"dbt --quiet ls --select state:modified+ --state ./.state --resource-type exposure --target {env}"
        )
    )
    logging.info(f"{impacted_exposures=}")
    emoji_map = {
        "analysis": "🕵",
        "application": "📱",
        "dashboard": "📊",
        "ml": "🤖",
        "notebook": "📓",
    }

    exposures_md_raw = []
    for exposure in impacted_exposures:
        exposure_metadata = manifest_json["exposures"][exposure.replace(":", ".")]
        exposures_md_raw.append(
            f"{emoji_map[exposure_metadata['type']]} {exposure_metadata['type'].upper()}|{exposure_metadata['label']}|{exposure_metadata['owner']['name']}|"
        )

    exposures_md = "\n".join(sorted(exposures_md_raw))
    logging.debug(f"{exposures_md=}")

    impacted_markdown = f"""Outputs from `manifest.json` comparison:

| Directly impacted models |
| - |
{direct_md}



| Indirectly impacted models |
| - |
{indirect_md}


Impacted exposures:

| Exposure type | Name | Owner |
| - | - | - |
{exposures_md}"""
    logging.debug(f"{impacted_markdown=}")
    delete_github_pr_bot_comments(
        pull_request_id=pull_request_id,
        env=env,
        identifier_text="Outputs from `manifest.json` comparison:",
    )
    if len(directly_impacted_models) > 0 or len(indirectly_impacted_models) > 0:
        send_github_pr_comment(pull_request_id, impacted_markdown)


@retry(tries=3, delay=5)
def fetch_results_from_bigquery(
    query_template: str, cicd_dataset: str, model_name: str
) -> list:
    """Run query across all environments in BigQuery and return results"""

    # Fetch dataset from manifest.json
    with open(f"./target/manifest.json") as f:
        manifest_json = json.load(f)

    for k, v in manifest_json["nodes"].items():
        if k.split(".")[-1] == model_name:
            dataset_id = v["schema"]

    results = []
    dataset_matrix = {
        "cicd": cicd_dataset,
        "other_envs": dataset_id,
    }
    service_account_matrix = {"cicd": "stg", "stg": "stg", "prd": "prd"}
    for env in ["cicd", "stg", "prd"]:
        if env == "cicd":
            dataset = dataset_matrix["cicd"]
        else:
            dataset = dataset_matrix["other_envs"]

        client = get_gcp_auth_clients(service_account_matrix[env])["bigquery"]

        query = Template(query_template).render(
            env=env, table_name=f"{client.project}.{dataset}.{model_name}"
        )
        logging.info(f"Running query on {dataset} dataset on {client.project}...")
        logging.debug(f"{query=}")

        # If we change column names or add new models we need to tolerate these not being comparable across environments
        # If this happens, the comment will only include data from where the updated column/name is present
        try:
            query_job = client.query(query)
            for row in query_job:
                i = dict(row.items())
                results.append(i)
        except (BadRequest, NotFound) as e:
            logging.info(f"{type(e)=}")

            assert (
                env != "cicd"
            ), f"Mart monitor for {model_name} failed on CICD dataset, likely due to an invalid query."

        logging.debug(f"{results=}")
    return results


def format_results(results: list) -> list:
    """Use local DuckDB engine to format the results for GitHub comment"""
    arrow_table = pa.Table.from_pylist(results)
    metric_names = [x for x in results[0].keys() if x != "table_name"]
    logging.debug(f"{metric_names=}")

    # Calculate the percentage difference for each metric across the environments
    metrics_base = " ".join(
        [
            f"""
            CASE
                WHEN table_name = 'cicd' THEN NULL
                ELSE ROUND(((CAST({metric} AS NUMERIC) / (SELECT {metric} FROM arrow_table WHERE table_name = 'cicd')) - 1) * 100, 1)
            END AS diff_{metric}_pct,
            """
            for metric in metric_names
        ]
    )

    # Use the percentage difference to format the data for a GitHub comment
    metrics_calc = " ".join(
        [
            f"""
        CASE
            WHEN table_name == 'cicd' THEN
                CASE
                    WHEN {metric} % 1 == 0 THEN {metric}
                    ELSE CAST({metric} AS NUMERIC)
                END
            ELSE
                CASE
                    WHEN ABS(diff_{metric}_pct) <= 0.5 THEN '🟢'
                    WHEN ABS(diff_{metric}_pct) < 1 THEN '🟡'
                    ELSE '🔴'
                END
                || ' '
                || ROUND(CAST({metric} AS NUMERIC), 0)
                || ' ('
                || ROUND(diff_{metric}_pct, 2)
                || '%)'
        END AS diff_{metric}_pct,"""
            for metric in metric_names
        ]
    )
    query = f"""
    WITH base AS (
        SELECT
            *,
            {metrics_base}
        FROM arrow_table
    )

    SELECT
        table_name,
        {metrics_calc}
    FROM base
    ORDER BY
        CASE
            WHEN table_name = 'cicd' THEN 1
            WHEN table_name = 'stg' THEN 2
            WHEN table_name = 'prd' THEN 3
        END
    """
    logging.debug(f"{query=}")
    con = duckdb.connect(database=":memory:")
    df = con.execute(query).arrow()

    # Format table to a list of rows, add column that details the metric names
    data = df.to_pydict()
    metrics_col = ["metrics"] + [x for x in results[0].keys() if x != "table_name"]
    for index, (k, v) in enumerate(data.items()):
        v.insert(0, metrics_col[index])

    logging.debug(f"{data=}")

    return data


def run_monitor(
    monitor: dict, dbt_dataset: str, pull_request_id: int, target_branch: str
) -> None:
    """Run a monitor and post comments to GitHub PR"""

    logging.info(
        f"{monitor['monitor_name']}: Starting process for {monitor['monitor_name']}..."
    )
    results = fetch_results_from_bigquery(
        query_template=monitor["query"],
        cicd_dataset=dbt_dataset,
        model_name=monitor["model_name"],
    )
    data = format_results(results)
    markdown_table = transform_list_to_markdown(data, monitor["monitor_name"])
    delete_github_pr_bot_comments(
        pull_request_id, target_branch, monitor["monitor_name"]
    )
    send_github_pr_comment(pull_request_id=pull_request_id, message=markdown_table)


def transform_list_to_markdown(input: list, monitor_name: str) -> str:
    """Transform a list into a table formatted as Markdown"""

    formatted_input = "".join(
        ["".join(x) for x in [v for k, v in input.items() if k != "table_name"]]
    )
    if (
        "".join(formatted_input).find("🟡") > 0
        or "".join(formatted_input).find("🔴") > 0
    ):
        headers = input["table_name"]
        value_matrix = [v for k, v in input.items() if k != "table_name"]
    else:
        headers = ["All values match!!"]
        value_matrix = [["👍👍"]]

    writer = pytablewriter.MarkdownTableWriter(
        table_name=monitor_name,
        headers=headers,
        value_matrix=value_matrix,
    )
    markdown_table = writer.__str__()
    logging.debug(f"{monitor_name}: {markdown_table=}")

    return markdown_table


def fetch_query_data_from_yml() -> List[Mapping[str, str]]:
    """Fetch data from yaml file."""
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__))
    )
    with open(os.path.join(__location__, "mart_monitor_queries.yml")) as f:
        query_data = yaml.safe_load(f)

    assert query_data is not None

    data = query_data["query_data"]
    for i in data:
        assert list(i.keys()) == ["monitor_name", "model_name", "query"]

    return data


def main() -> None:
    set_logging_options()

    dbt_dataset, pull_request_id, target_branch = parse_command_line_args()

    try:
        download_manifest_json(
            env=target_branch,
            destination_file_name="./target/manifest.json",
            version="previous",
        )
    except ManifestInitRunError:
        init_run = True

    if (
        "init_run" not in locals()
    ):  # i.e. on inital run no manifest.json to compare with so need to skip
        try:
            if target_branch == "stg":
                # Monitors only runs for PRs to `stg` branch
                monitor_yaml = fetch_query_data_from_yml()

                # Run monitors in parallel
                pool = ThreadPool(8)
                pool.starmap(
                    run_monitor,
                    [
                        (monitor, dbt_dataset, pull_request_id, target_branch)
                        for monitor in monitor_yaml
                    ],
                )

        except (
            Exception
        ) as e:  # This script failing should not block the CI pipeline, hence this generic error handling
            logging.info(f"{e=}")

        try:
            _, pull_request_id, target_branch = parse_command_line_args()
            compare_manifests_and_comment_impacted_models(
                env=target_branch,
                manifest_file_name="./.state/manifest.json",
                pull_request_id=pull_request_id,
            )
        except (
            Exception
        ) as e:  # This script failing should not block the CI pipeline, hence this generic error handling
            logging.info(f"{e=}")


if __name__ == "__main__":
    main()
