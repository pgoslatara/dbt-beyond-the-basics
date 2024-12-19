{{ config(persist_docs={"relation": true, "columns": false}) }}

select *
from {{ ref("int_orders") }}
