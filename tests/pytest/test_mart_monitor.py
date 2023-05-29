import pytest


@pytest.mark.no_deps
def test_mart_monitor_keys(mart_monitor_queries_yml: dict) -> None:
    """
    Monitors must contains the following keys: monitor_name, model_name, query
    """

    for monitor in mart_monitor_queries_yml["query_data"]:
        assert list(monitor.keys()) == [
            "monitor_name",
            "model_name",
            "query",
        ], f"Monitor {monitor['monitor_name']} must contains the follwoing keys: monitor_name, model_name, query."


@pytest.mark.no_deps
def test_mart_monitor_query_has_table_name(
    mart_monitor_queries_yml: dict,
) -> None:
    """
    Monitors must contain the table name as a column
    """

    for monitor in mart_monitor_queries_yml["query_data"]:
        assert (
            monitor["query"].find("'{{ env }}' as table_name,") > 0
        ), f"Query for `{monitor['monitor_name']}` must contain `'{{ env }}' as table_name,`."
