
    
    

with dbt_test__target as (

  select block_hash as unique_field
  from `beyond-basics-dev`.`dbt_padraic_marts_crypto`.`fct_bitcoin_blocks`
  where block_hash is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


