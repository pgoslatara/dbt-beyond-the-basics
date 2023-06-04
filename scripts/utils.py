import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, List, Mapping, Optional, Union

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from retry import retry
from sh import dbt


class GitHubAPIRateLimitError(Exception):
    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return f"GitHubAPIRateLimitError: API allocations resets at {self.get_api_reset_time()}."

    def get_api_reset_time(self) -> datetime:
        r = call_github_api(
            "GET",
            "rate_limit",
        )
        return datetime.fromtimestamp(r["rate"]["reset"])


class ManifestInitRunError(Exception):
    pass


def call_github_api(
    method: str,
    endpoint: str,
    data: Optional[Mapping[str, Union[int, str]]] = None,
    params: Optional[Mapping[str, Union[int, str]]] = None,
) -> Any:
    url = f"https://api.github.com/{endpoint}"
    logging.debug(f"Calling {url}...")
    r = requests.request(
        method=method.upper(),
        url=url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        params=params,
        data=json.dumps(data),
    )

    r.raise_for_status()

    if r.status_code == 204:
        return {"success": True}
    elif (
        isinstance(r.json(), dict)
        and r.json().get("message")
        and r.json()["message"].startswith("API rate limit exceeded for user ID")
    ):
        raise GitHubAPIRateLimitError
    else:
        return r.json()


def delete_github_pr_bot_comments(
    pull_request_id: int, env: str, identifier_text: str
) -> None:
    """Delete all comments on a PR from specified bot containing a specific text string"""

    page = 1
    comments_data = get_all_github_pr_comments(pull_request_id)

    bot_comments = [
        x
        for x in comments_data
        if x["body"].find(identifier_text) >= 0
        and x["user"]["login"] == "github-actions[bot]"
    ]
    logging.debug(f"Retrieved {len(bot_comments)} comments from bot...")

    for comment in bot_comments:
        delete_github_pr_comment(comment["id"])


def delete_github_pr_comment(comment_id: int) -> None:
    """Delete a comment from a PR on GitHub"""

    logging.info(f"Deleting comment_id {comment_id}...")

    response = call_github_api(
        method="DELETE",
        endpoint=f"repos/pgoslatara/dbt-beyond-the-basics/issues/comments/{comment_id}",
    )

    assert response["success"] is True


def download_manifest_json(
    env: str, destination_file_name: str, version: str = "latest"
) -> None:
    """Download the latest or previous manifest.json from GCS"""

    assert version in {
        "latest",
        "previous",
    }, "`version` must be 'latest' or 'previous'."

    storage_client = get_gcp_auth_clients(env)["storage"]
    blobs = storage_client.list_blobs(f"beyond-basics-dbt-manifests-{env}")
    valid_blobs = []
    for blob in blobs:
        re_compile = re.compile(
            r"uploaded_at=[0-9 -.:]*\/github_run_number=([0-9]*)\/manifest\.json"
        ).match(blob.name)
        if "group" in dir(re_compile):
            valid_blobs.append({"blob": blob, "build_number": int(re_compile[1])})

    logging.info(f"Found {len(valid_blobs)} valid blobs...")

    if len(valid_blobs) == 0:
        raise ManifestInitRunError
    else:
        build_numbers = sorted([x["build_number"] for x in valid_blobs], reverse=True)

        if version == "latest":
            index = 0
        elif version == "previous":
            index = 1

        manifest_blob = [
            x["blob"] for x in valid_blobs if x["build_number"] == build_numbers[index]
        ][0]
        Path(destination_file_name[: destination_file_name.rfind("/")]).mkdir(
            parents=True, exist_ok=True
        )
        manifest_blob.download_to_filename(destination_file_name)
        logging.info(
            f"Downloaded {version} manifest from {manifest_blob.name} to {destination_file_name}"
        )


def get_all_github_pr_comments(pull_request_id: int) -> dict:
    """Retrieve all comments from a GitHub PR"""

    page_num = 1
    pr_comments = []

    while True:
        logging.info(f"Retrieving comments on PR {pull_request_id}: page {page_num}...")
        new_comments = call_github_api(
            method="GET",
            endpoint=f"repos/pgoslatara/dbt-beyond-the-basics/issues/{pull_request_id}/comments",
            params={"page": page_num, "per_page": 100},
        )
        page_num += 1
        if new_comments == []:
            break
        else:
            pr_comments += new_comments

    logging.debug(f"Retrieved {len(pr_comments)} comments...")
    return pr_comments


def get_gcp_auth_clients(env: str) -> dict:
    """
    Return an authenticated client object for supported GCP products

    Order of preference for authentication:
        1. Service account key file specific to an environment, e.g. service_account_stg.json
        2. Service account key file not specific to an environment, e.g. service_account.json
        3. Local credentials, i.e. via gcloud CLI
    """

    project_id = f"beyond-basics-{env}"

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__))
    )
    service_account_key_env_path = os.path.join(
        __location__[: __location__.rfind("/")],
        f"service_account_{env}.json",
    )
    service_account_key_path = os.path.join(
        __location__[: __location__.rfind("/")],
        f"service_account.json",
    )
    if Path(service_account_key_env_path).exists():
        logging.debug(
            f"Service account key specific to an env file exists, using {service_account_key_env_path} for auth..."
        )
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_env_path
        credentials = service_account.Credentials.from_service_account_file(
            service_account_key_env_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client_bigquery = bigquery.Client(
            credentials=credentials,
            project=project_id,
        )
        client_storage = storage.Client(credentials=credentials)
    elif Path(service_account_key_path).exists():
        logging.debug(
            f"Service account key file exists, using {service_account_key_path} for auth..."
        )
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_path
        credentials = service_account.Credentials.from_service_account_file(
            service_account_key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client_bigquery = bigquery.Client(
            credentials=credentials,
            project=project_id,
        )
        client_storage = storage.Client(credentials=credentials)
    else:
        logging.debug(
            f"Service account key file does not exist, using default credentials for auth..."
        )
        client_bigquery = bigquery.Client(
            project=project_id,
        )
        client_storage = storage.Client()

    return {
        "bigquery": client_bigquery,
        "storage": client_storage,
    }


def run_dbt_shell_command(
    command: str, foreground: bool = False
) -> Optional[Union[List[str], None]]:
    """
    Runs dbt on the command line anre returns the output as a non-null newline-delimited list of strings
    """

    logging.info(f"Running: {command}...")

    if foreground is False:
        return [x for x in dbt(command.split(" ")[1:]).split("\n") if x != ""]
    else:
        dbt(command.split(" ")[1:], _fg=foreground)


def send_github_pr_comment(pull_request_id: int, message: str) -> str:
    """Create a comment on a GitHub PR."""

    response = call_github_api(
        method="POST",
        endpoint=f"repos/pgoslatara/dbt-beyond-the-basics/issues/{pull_request_id}/comments",
        data={"body": message},
    )

    logging.info(f"Comment URL: {response['html_url']}")

    return response["html_url"]


def set_logging_options() -> None:
    """Set basic logging options"""
    LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
    )

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)


@retry(tries=3, delay=5)
def upload_to_gcs(
    env: str, bucket_name: str, upload_directory: str, file_to_upload: str
):
    """Upload a file to a Google Cloud Storage bucket"""

    client = get_gcp_auth_clients(env)["storage"]
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{upload_directory}/{file_to_upload.split('/')[-1]}")
    logging.info(f"Uploading {file_to_upload} to {blob.name} in {blob.bucket.name}...")
    blob.upload_from_filename(file_to_upload)
