import json
from pathlib import Path

import pytest
import yaml


@pytest.fixture(scope="module")
def catalog_json() -> dict:
    with Path("./target/catalog.json").open() as f:
        data = json.load(f)
    return data


@pytest.fixture(scope="module")
def manifest_json() -> dict:
    with Path("./target/manifest.json").open() as f:
        data = json.load(f)
    return data


@pytest.fixture(scope="module")
def mart_monitor_queries_yml() -> dict:
    with Path("./scripts/mart_monitor_queries.yml").open() as f:
        data = yaml.safe_load(f)
    return data


@pytest.fixture(scope="module")
def run_results_json() -> dict:
    with Path("./target/run_results.json").open() as f:
        data = json.load(f)
    return data


@pytest.fixture(scope="module")
def sources_json() -> dict:
    with Path("./target/sources.json").open() as f:
        data = json.load(f)
    return data
