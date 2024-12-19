import mermaid as mmd
from click import Command, Context
from dbterd import default
from dbterd.adapters.base import Executor
from mermaid.graph import Graph

erd = Executor(Context(Command(name="run"))).run(
    algo=default.default_algo(),
    artifacts_dir="./target",
    dbt="--select",
    entity_name_format=default.default_entity_name_format(),
    exclude=[],
    output="./target",
    params={},
    select=["path:models/marts"],
    target="mermaid",
)

mermaid = mmd.Mermaid(Graph("erDiagram", erd))
mermaid.to_png("./target/mermaid.png")
