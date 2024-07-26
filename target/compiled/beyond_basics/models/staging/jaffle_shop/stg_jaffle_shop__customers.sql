with
    source as (
        select * from `beyond-basics-dev`.`dbt_padraic_seeds_jaffle_shop`.`seed_jaffle_shop__customers`

    ),

    renamed as (
        select
            id as customer_id,
            first_name,
            last_name,
            concat(first_name, " ", last_name) as full_name
        from source
    )

select *
from renamed