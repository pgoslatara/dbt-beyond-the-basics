
    
    

with dbt_test__target as (

  select customer_id as unique_field
  from `beyond-basics-dev`.`dbt_padraic_intermediate_finance`.`int_customers`
  where customer_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


