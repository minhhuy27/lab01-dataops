
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    group by status

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6'
)


