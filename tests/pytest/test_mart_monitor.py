import pytest


@pytest.mark.no_deps
def test_mart_monitor_keys(mart_monitor_queries_yml: dict) -> None:
    """
    Checks must contains the following keys: check_name, model_name, query
    """

    for check in mart_monitor_queries_yml["query_data"]:
        assert list(check.keys()) == [
            "check_name",
            "model_name",
            "query",
        ], f"Check {check['check_name']} must contains the follwoing keys: check_name, model_name, query."


@pytest.mark.no_deps
def test_mart_monitor_query_has_table_name(
    mart_monitor_queries_yml: dict,
) -> None:
    """
    Checks must contain the table name as a column
    """

    for check in mart_monitor_queries_yml["query_data"]:
        assert (
            check["query"].find("'{{ env }}' as table_name,") > 0
        ), f"Query for `{check['check_name']}` must contain `'{{ env }}' as table_name,`."
