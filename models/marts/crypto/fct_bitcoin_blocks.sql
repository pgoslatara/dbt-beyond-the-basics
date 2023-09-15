{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'data_type': 'timestamp', 'field': 'created_at', 'granularity': 'day'}
    )
}}

select *
from {{ ref('stg_public_datasets__bitcoin_blocks') }}
