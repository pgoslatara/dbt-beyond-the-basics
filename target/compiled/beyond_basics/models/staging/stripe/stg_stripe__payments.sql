with
    source as (
        select * from `beyond-basics-dev`.`dbt_padraic_seeds_stripe`.`seed_stripe__payments`

    ),

    renamed as (

        select
            id as payment_id,
            order_id,
            payment_method,
            case
                when payment_method in ("coupon", "gift_card") then true else false
            end as is_voucher,

            -- `amount` is currently stored in cents, so we convert it to dollars
             (amount / 100)  as amount

        from source

    )

select *
from renamed