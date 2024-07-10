import json
import re
from pathlib import Path

import pytest
import yaml


@pytest.mark.no_deps
def test_model_contains_ifnull() -> None:
    """
    Our convention is to use COALESCE rather than IFNULL.
    More info: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
    """

    macro_files = list(Path("./macros").rglob("*.sql"))
    model_files = list(Path("./models").rglob("*.sql"))
    test_files = list(Path("./tests").rglob("*.sql"))

    for f in macro_files + model_files + test_files:
        with open(f) as file:
            assert (
                "ifnull" not in file.read().lower()
            ), f"Rewrite {f} to use `coalesce` in place of `ifnull`."


@pytest.mark.manifest_json
def test_model_has_unique_test(manifest_json: dict) -> None:
    """
    All models should at a minimum have a test of uniqueness on primary/composite key.
    """

    # Determine models in beyond_basics package
    model_names = [
        k
        for k, v in manifest_json["nodes"].items()
        if (k.startswith("model.beyond_basics") or k.startswith("seed.beyond_basics"))
    ]

    models_with_unique_combination_of_columns_test = []
    models_with_unique_test = []
    for k, v in manifest_json["nodes"].items():
        if k.startswith("test"):
            if len(v["depends_on"]["nodes"]) > 0:
                if v["test_metadata"]["name"] == "unique_combination_of_columns":
                    models_with_unique_combination_of_columns_test.append(
                        v["depends_on"]["nodes"][0]
                    )
                elif v["test_metadata"]["name"] == "unique":
                    models_with_unique_test.append(v["depends_on"]["nodes"][0])

    for model in model_names:
        if (
            model in models_with_unique_combination_of_columns_test
            or model in models_with_unique_test
        ):
            pass
        else:
            raise AssertionError(
                f"{model} does not have a `unique` or `unique_combination_of_columns` test."
            )


@pytest.mark.no_deps
def test_model_intermediate_correct_locations() -> None:
    """
    Models in 'intermediate' need to be in sub-directory.

    This tests operates directly on file names, as such it has no prior dependencies.
    """

    sql_files = [x for x in Path("./models/intermediate").rglob("*.sql")]

    for f in sql_files:
        rel_dir = f.parts[f.parts.index("intermediate") + 1 :]
        assert (
            len(rel_dir) == 2
        ), "Models cannot be directly in 'intermediate', they must be in a sub-directory, e.g. 'staging/intermediate/model.sql'."


@pytest.mark.no_deps
def test_model_marts_correct_locations() -> None:
    """
    Models in 'marts' need to be in sub-directory.

    This tests operates directly on file names, as such it has no prior dependencies.
    """

    sql_files = [x for x in Path("./models/marts").rglob("*.sql")]

    for f in sql_files:
        rel_dir = f.parts[f.parts.index("marts") + 1 :]
        assert (
            len(rel_dir) == 2
        ), "Models cannot be directly in 'marts', they must be in a sub-directory, e.g. 'staging/marts/model.sql'."


@pytest.mark.manifest_json
def test_models_marts_must_have_monitors(manifest_json: dict) -> None:
    """As marts are exposed to external users we should have at least one monitor on them in `mart_monitor_queries.yml`."""

    mart_models = [
        k.split(".")[-1]
        for k, v in manifest_json["nodes"].items()
        if "marts" in v["config"]["tags"]
    ]

    with open(
        Path(Path(__file__).parent.parent.parent, "scripts/mart_monitor_queries.yml")
    ) as f:
        query_data = yaml.safe_load(f)

    models_with_monitors = [x["model_name"] for x in query_data["query_data"]]

    for mart in mart_models:
        assert (
            mart in models_with_monitors
        ), f"{mart} does not have an associated monitor in `mart_monitor_queries.yml`."


@pytest.mark.no_deps
def test_model_names() -> None:
    """
    Model names should comform to naming conventions.

    This tests operates directly on file names, as such it has no prior dependencies.
    """

    sql_files = [x for x in Path("./models").rglob("*.sql")]

    for f in sql_files:
        rel_dir = f.parts[f.parts.index("models") + 1 :]

        if ("staging", "intermediate") in rel_dir:
            assert (
                str(f.absolute()).count("__") == 1
            ), f"File name is required to contain a single occurrence of '__': {str(f.absolute())}"

        if "staging" in rel_dir:
            if len(rel_dir) == 3:
                # 1 level hierarchy
                regex_pattern = rf"(base|stg)_{rel_dir[-2]}__[a-z0-9_]*\.(sql){{1}}"
            elif len(rel_dir) == 4:
                # 2 level hierarchy
                regex_pattern = (
                    rf"(base|stg)_{rel_dir[-3]}_{rel_dir[-2]}__[a-z0-9_]*\.(sql){{1}}"
                )
            else:
                raise ValueError(
                    "./models/staging only support 1 or 2 directory levels."
                )

        elif "intermediate" in rel_dir:
            assert (
                len(rel_dir) == 3
            ), "./models/intermediate only support 1 directory level."
            regex_pattern = r"int_[a-z0-9_]*\.(sql){1}"

        elif "marts" in rel_dir:
            assert len(rel_dir) == 3, "./models/marts only support 1 directory level."

            regex_pattern = r"(dim|fct|rpt)_[a-z_0-9]*\.(sql){1}"

        elif "utilities" in rel_dir:
            assert len(rel_dir) == 2, "./models/utilities do not support subdirectories"

            regex_pattern = r"utilities__[a-z_0-9]*\.(sql){1}"

        assert "group" in dir(
            re.search(regex_pattern, f.name)
        ), f"No returned groups, file name does not conform to naming convention: {f.name}"
        assert (
            f.name == re.compile(regex_pattern).match(f.name)[0]
        ), f"File name does not conform to naming convention: {f.name}"

    for f in list(Path("./seeds").rglob("*.csv")):
        rel_dir = f.parts[f.parts.index("seeds") + 1 :]
        assert len(rel_dir) == 2, "./seeds only support 1 directory level."

        regex_pattern = rf"seed_{rel_dir[-2]}__[a-z0-9_]*\.(csv){{1}}"
        assert "group" in dir(
            re.search(regex_pattern, f.name)
        ), f"No returned groups, file name does not conform to naming convention: {f.name}"
        assert (
            f.name == re.compile(regex_pattern).match(f.name)[0]
        ), f"File name does not conform to naming convention: {f.name}"


@pytest.mark.no_deps
def test_model_subdir_names() -> None:
    """
    Subdirectories under "./models" should conform to dbt recommendations.

    This tests operates directly on file names, as such it has no prior dependencies.
    """

    dbt_model_subdirs = [
        "intermediate",
        "marts",
        "staging",
        "utilities",
    ]

    for x in Path("./models").glob("*"):
        if x.is_dir():
            assert (
                x.stem in dbt_model_subdirs
            ), f"{x.stem} is not in dbt's recommended subdirectories in ./models."


@pytest.mark.no_deps
def test_model_staging_correct_locations() -> None:
    """
    Models in 'staging' need to be in a nested sub-directory.

    This tests operates directly on file names, as such it has no prior dependencies.
    """

    sql_files = [x for x in Path("./models/staging").rglob("*.sql")]

    for f in sql_files:
        rel_dir = f.parts[f.parts.index("staging") + 1 :]
        assert (
            len(rel_dir) == 2
        ), "Models cannot be directly in 'staging', they must be in a nested sub-directory or a second sub-directory, e.g. 'staging/stripe/model.sql'."
