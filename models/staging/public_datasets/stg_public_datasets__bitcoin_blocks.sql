{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'data_type': 'date', 'field': 'timestamp_date', 'granularity': 'day'}
    )
}}

select distinct
    `hash` as block_hash,
    timestamp as created_at,
    date(timestamp) as timestamp_date,
    * except (`hash`, timestamp, timestamp_month)
from {{ source('crypto_bitcoin', 'blocks') }}
where
    timestamp <= timestamp_trunc(current_timestamp(), day)

    {% if is_incremental() %}

        and timestamp >= timestamp(date_sub(current_date(), interval 1 day))

    {% endif %}

    {# minimize data usage for dev #}
    {% if not target.name in ['stg', 'prod'] or env_var('DBT_CICD_RUN', 'false') == 'true' %}

        and timestamp_month
        >= date_trunc(date_sub(current_date(), interval 1 month), month)

    {% endif %}

    and timestamp_month >= "2020-01-01"
