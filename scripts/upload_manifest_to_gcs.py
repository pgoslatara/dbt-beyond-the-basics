import argparse
import logging
import os
from datetime import datetime

from git import Repo
from utils import call_github_api, set_logging_options, upload_to_gcs


def get_latest_commit_hash(target_branch: str) -> str:
    """Retrieve the latest commit hash from a branch"""

    r = call_github_api(
        method="GET",
        endpoint=f"repos/pgoslatara/dbt-beyond-the-basics/branches/{target_branch}",
    )

    head_hash = r["commit"]["sha"]
    logging.info(f"Latest commit hash on {target_branch}: {head_hash}")
    return head_hash


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

    github_run_number = os.getenv("GITHUB_RUN_NUMBER", "local_run")
    logging.info(f"{github_run_number=}")

    repo = Repo(".")
    current_branch_head_hash = repo.commit().hexsha
    logging.info(f"{current_branch_head_hash=}")

    if current_branch_head_hash == get_latest_commit_hash(target_branch):
        # Workflows can be re-run, if the HEAD of the branch in the workflow is not the HEAD of the target_branch then no upload is performed.
        logging.info(
            "Current branch is the HEAD of the target branch, proceeding to upload..."
        )
        upload_to_gcs(
            env=target_branch,
            bucket_name=f"beyond-basics-dbt-manifests-{target_branch}",
            upload_directory=f"uploaded_at={datetime.utcnow()}/github_run_number={github_run_number}",
            file_to_upload="./target/manifest.json",
        )
    else:
        logging.info(
            "Current branch is not at the the HEAD of the target branch, upload cancelled."
        )


if __name__ == "__main__":
    main()
