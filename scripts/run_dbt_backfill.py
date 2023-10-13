import argparse
import logging
import os

from retry import retry
from utils import (
    ManifestInitRunError,
    call_github_api,
    download_manifest_json,
    run_dbt_command,
    send_github_pr_comment,
    set_logging_options,
)


@retry(tries=3, delay=5)
def run_dbt_backfill(env: str) -> None:
    """
    Download the previous version of manifest.json from GCS and use dbt's "--state" flag identify modified nodes.
    If there are modified nodes, fully refresh them and their downstream nodes.
    """

    # Download previous manifest.json to ./.state directory
    download_manifest_json(
        env=env, destination_file_name="./.state/manifest.json", version="previous"
    )

    # Download latest manifest.json to ./target directory
    download_manifest_json(
        env=env, destination_file_name="./target/manifest.json", version="latest"
    )

    # List modified nodes
    modified_nodes_raw = run_dbt_command(
        f"dbt ls --select state:modified,package:beyond_basics --state ./.state --resource-type model --target {env}"
    )

    if (
        modified_nodes_raw[0].find(
            "The selection criterion 'state:modified,package:beyond_basics' does not match any nodes"
        )
        == -1
    ):
        modified_nodes_clean = [x.split(".")[-1] for x in modified_nodes_raw]
        logging.info(f"{modified_nodes_clean=}")

        workflow_url = f"https://github.com/pgoslatara/dbt-beyond-the-basics/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
        logging.info(f"{workflow_url=}")

        # Identify the PR associated with the head commit
        pull_request_id = call_github_api(
            method="GET",
            endpoint=f"repos/pgoslatara/dbt-beyond-the-basics/commits/{os.getenv('GITHUB_SHA')}/pulls",
        )[0]["number"]

        logging.info(f"{pull_request_id=}")
        send_github_pr_comment(
            pull_request_id=pull_request_id,
            message=f"The [CD pipeline]({workflow_url}) has started the backfill process...",
        )

        # Fully refresh modified nodes and their downstream dependencies
        run_dbt_command(
            f"dbt build --select state:modified+,package:beyond_basics --state ./.state --full-refresh --target {env}"
        )

        send_github_pr_comment(
            pull_request_id=pull_request_id,
            message=f"The [CD pipeline]({workflow_url}) has successfully finished the backfill process 🎉.",
        )
    else:
        logging.info("No nodes modified, no backfill required.")


def main() -> None:
    set_logging_options()

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-branch",
        help="The branch that has been merged into",
        required=True,
    )
    args = parser.parse_args()

    target_branch = args.target_branch
    logging.info(f"{target_branch=}")

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
    ):  # i.e. on initial run no manifest.json to compare with so need to skip
        run_dbt_backfill(target_branch)


if __name__ == "__main__":
    main()
