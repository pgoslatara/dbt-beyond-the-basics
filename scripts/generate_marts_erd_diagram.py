import mermaid as mmd
from dbterd import default
from dbterd.api import DbtErd
from mermaid.graph import Graph

erd = DbtErd(
    algo=default.default_algo(),
    artifacts_dir="./target",
    dbt="--select",
    entity_name_format=default.default_entity_name_format(),
    exclude=[],
    output="./target",
    params={},
    select=["path:models/marts"],
    target="mermaid",
).get_erd()

mermaid = mmd.Mermaid(Graph("erDiagram", erd))
mermaid.to_png("./target/mermaid.png")
