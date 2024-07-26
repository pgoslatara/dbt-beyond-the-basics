
    
    

with dbt_test__target as (

  select id as unique_field
  from `beyond-basics-dev`.`dbt_padraic_seeds_jaffle_shop`.`seed_jaffle_shop__orders`
  where id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


