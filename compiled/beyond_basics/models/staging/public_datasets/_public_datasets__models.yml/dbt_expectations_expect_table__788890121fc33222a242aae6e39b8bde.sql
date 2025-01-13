



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 1
)
 as expression


    from `beyond-basics-prd`.`staging_public_datasets`.`stg_public_datasets__bitcoin_blocks`
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors





