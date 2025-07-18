import json
from pathlib import Path

import mermaid as mmd
from dbterd import default
from dbterd.api import DbtErd
from mermaid.graph import Graph

artifacts_dir = "./target"

# Super ugly workaround because `dbt-artifacts-parser` struggles with the latest `catalog.json` from dbt
with Path.open(f"{artifacts_dir}/catalog.json", "r") as f:
    catalog_data = json.load(f)

catalog_data["metadata"].pop("invocation_started_at")

with Path.open(f"{artifacts_dir}/catalog.json", "w") as f:
    json.dump(catalog_data, f)


erd = DbtErd(
    algo=default.default_algo(),
    artifacts_dir=artifacts_dir,
    dbt="--select",
    entity_name_format=default.default_entity_name_format(),
    exclude=[],
    output=artifacts_dir,
    params={},
    select=["path:models/marts"],
    target="mermaid",
).get_erd()

mermaid = mmd.Mermaid(Graph("erDiagram", erd))
mermaid.to_png(f"{artifacts_dir}/mermaid.png")
