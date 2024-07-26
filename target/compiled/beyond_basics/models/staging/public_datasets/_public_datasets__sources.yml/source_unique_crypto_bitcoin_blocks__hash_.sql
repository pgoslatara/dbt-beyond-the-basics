
    
    

with dbt_test__target as (

  select `hash` as unique_field
  from `bigquery-public-data`.`crypto_bitcoin`.`blocks`
  where `hash` is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


