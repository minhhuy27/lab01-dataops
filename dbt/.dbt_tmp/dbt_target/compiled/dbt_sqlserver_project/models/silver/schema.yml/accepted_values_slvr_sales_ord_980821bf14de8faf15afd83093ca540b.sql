
    
    

with all_values as (

    select
        order_channel as value_field,
        count(*) as n_records

    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    group by order_channel

)

select *
from all_values
where value_field not in (
    'Online','Offline'
)


