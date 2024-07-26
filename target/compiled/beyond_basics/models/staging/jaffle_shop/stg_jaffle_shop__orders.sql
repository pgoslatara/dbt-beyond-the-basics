with
    source as (
        select * from `beyond-basics-dev`.`dbt_padraic_seeds_jaffle_shop`.`seed_jaffle_shop__orders`

    ),

    renamed as (

        select id as order_id, user_id as customer_id, order_date, status from source

    )

select *
from renamed