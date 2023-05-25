import pytest


@pytest.mark.run_results_json
def test_total_build_duration(run_results_json: dict) -> None:
    """
    As part of CICD we run "dbt build ...". As the CICD pipeline runs multiple times a day and developers await it's completion it is important that it completes in a few minutes to provide developers with feedback and avoid delaying releases and hotfixes.

    This test needs to run after run_results.json is built (i.e. `dbt build`).
    """

    MAX_ALLOWABLE_BUILD_DURATION_MINUTES = 3

    total_execution_time_minutes = run_results_json["elapsed_time"] / 60

    assert (
        total_execution_time_minutes < MAX_ALLOWABLE_BUILD_DURATION_MINUTES
    ), f"This CICD run took {total_execution_time_minutes:.2f} minutes, this is above the maximum premissible duration of {MAX_ALLOWABLE_BUILD_DURATION_MINUTES} minutes."


@pytest.mark.run_results_json
def test_total_bytes_scanned(run_results_json: dict) -> None:
    """
    As part of CICD we run "dbt build ...". As the CICD pipeline runs multiple times a day it is important that it does not scan an unecessarily large amount of data (this can be prevented using jinja if statements)

    This test needs to run after run_results.json is built (i.e. `dbt build`).
    """

    MAX_ALLOWABLE_GB_SCANNED = 50

    total_bytes_scanned = 0
    for result in run_results_json["results"]:
        bytes_processed = result["adapter_response"].get("bytes_processed", 0)
        total_bytes_scanned += bytes_processed

    total_gb_scanned = total_bytes_scanned / (1024**3)

    assert (
        total_gb_scanned < MAX_ALLOWABLE_GB_SCANNED
    ), f"This CICD run scanned {total_gb_scanned:.2f} GB, this is above the maximum premissible value of {MAX_ALLOWABLE_GB_SCANNED} GB."
