
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from `beyond-basics-dev`.`dbt_padraic_staging_stripe`.`stg_stripe__payments`
    group by payment_method

)

select *
from all_values
where value_field not in (
    'credit_card','coupon','bank_transfer','gift_card'
)


