
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `beyond-basics-dev`.`dbt_padraic_staging_jaffle_shop`.`stg_jaffle_shop__orders`
    group by status

)

select *
from all_values
where value_field not in (
    'placed','shipped','completed','return_pending','returned'
)


