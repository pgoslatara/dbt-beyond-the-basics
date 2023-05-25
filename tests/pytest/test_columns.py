import re
from pathlib import Path

import pytest


@pytest.mark.catalog_json
def test_column_names_models(catalog_json: dict) -> None:
    """
    Column names should only contain lowercase, underscore and integer characters.

    This test needs to run after catalog.json is built (i.e. `dbt docs generate`).
    """

    regex_pattern = "[a-z_0-9]*"

    for k, v in catalog_json["nodes"].items():
        for col in v["columns"].keys():
            # If a model has a RECORD column, catalog.json will contain the nested columns.
            # We do not apply this pytest to these columns as this would require all nested columns to be renamed, this isn't practical.
            if col.find(".") <= 0:
                assert (
                    col == re.compile(regex_pattern).match(col)[0]
                ), f"Column '{col}' in {k} does not align with the existing naming convention ({regex_pattern})."


@pytest.mark.no_deps
def test_column_names_seeds() -> None:
    """
    Column names should only contain lowercase, underscore and integer characters.
    """

    seed_files = list(Path("./seeds").rglob("*.csv"))

    regex_pattern = "[a-z_0-9]*"

    for f in seed_files:
        column_names = f.read_text().split("\n")[0].split(",")

        for col in column_names:
            assert (
                col == re.compile(regex_pattern).match(col)[0]
            ), f"Column '{col}' in {f.name} does not align with the existing naming convention ({regex_pattern})."
