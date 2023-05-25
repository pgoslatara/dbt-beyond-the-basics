import pytest


@pytest.mark.manifest_json
def test_lineage_intermediate_upstream(manifest_json: dict) -> None:
    """
    Intermediate models can only read from seeds, staging models and other intermediate models.
    """

    for k, v in manifest_json["nodes"].items():
        if "intermediate" in v["config"]["tags"]:
            for upstream_node in v["depends_on"]["nodes"]:
                assert (
                    any(
                        tag in manifest_json["nodes"][upstream_node]["config"]["tags"]
                        for tag in ["intermediate", "staging", "utilities"]
                    )
                    or manifest_json["nodes"][upstream_node]["resource_type"] == "seed"
                ), f"{k} depends on a node ({upstream_node}) that is not a seed or a staging or intermediate model, this is not permitted"


@pytest.mark.manifest_json
def test_lineage_marts_upstream(manifest_json: dict) -> None:
    """
    Mart models can only read from staging and intermediate models.
    """

    for k, v in manifest_json["nodes"].items():
        if "marts" in v["config"]["tags"]:
            for upstream_node in v["depends_on"]["nodes"]:
                assert any(
                    tag in manifest_json["nodes"][upstream_node]["config"]["tags"]
                    for tag in ["intermediate", "staging"]
                ), f"{k} depends on a node ({upstream_node}) that is not a staging or intermediate model, this is not permitted"


@pytest.mark.manifest_json
def test_lineage_sources_are_accessed_by_one_staging_model(manifest_json: dict) -> None:
    """
    Sources should only be read by one staging model (either base_*, stg_* or utilities_*).
    """

    for k, v in manifest_json["child_map"].items():
        source_family = k.split(".")[2]
        source_name = k.split(".")[-1]

        if k.split(".")[0:2] == ["source", "beyond_basics"]:
            downstream_models = list({x for x in v if x.startswith("model.")})
            assert downstream_models[0].startswith(
                (
                    "model.beyond_basics.base_",
                    "model.beyond_basics.stg_",
                    "model.beyond_basics.utilities_",
                )
            ), f"Source {source_family}.{source_name} is read by a model that is not base_*, stg_* or utilities_*"


@pytest.mark.manifest_json
def test_lineage_sources_are_orphaned(manifest_json: dict) -> None:
    """
    Sources should be read by downstream models, if not they may be abandoned and should be removed from the codebase.
    """

    for k, v in manifest_json["child_map"].items():
        if k.startswith("source.beyond_basics."):
            assert (
                len(v) > 0
            ), f"Source {k} is not accessed by any downstream model and should be removed from the codebase."


@pytest.mark.manifest_json
def test_lineage_staging_upstream(manifest_json: dict) -> None:
    """
    Staging models can only read from seeds, sources, base_* models or model from models from other packages (i.e. Fivetran packages).
    """

    for k, v in manifest_json["nodes"].items():
        if "staging" in v["config"]["tags"]:
            for upstream_node in v["depends_on"]["nodes"]:
                assert (
                    upstream_node.split(".")[0]
                    in [
                        "seed",
                        "source",
                    ]
                    or upstream_node.startswith("model.beyond_basics.base_")
                    or upstream_node.split(".")[1] != "beyond_basics"
                    or "utilities"
                    in manifest_json["nodes"][upstream_node]["config"]["tags"]
                ), f"{k} depends on a node ({upstream_node}) that is not a seed, source or base_* model, this is not permitted"
