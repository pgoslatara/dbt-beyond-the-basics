

select distinct
    `hash` as block_hash,
    timestamp as created_at,
    date(timestamp) as timestamp_date,
    * except (`hash`, timestamp, timestamp_month)
from `bigquery-public-data`.`crypto_bitcoin`.`blocks`
where
    timestamp <= timestamp_trunc(current_timestamp(), day)

    

        and timestamp >= timestamp(date_sub(current_date(), interval 1 day))

    

    
    

        and timestamp_month
        >= date_trunc(date_sub(current_date(), interval 1 month), month)

    

    and timestamp_month >= "2020-01-01"