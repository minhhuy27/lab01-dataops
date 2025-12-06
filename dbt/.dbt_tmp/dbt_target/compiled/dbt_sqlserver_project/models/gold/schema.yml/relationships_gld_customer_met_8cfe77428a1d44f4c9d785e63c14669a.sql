
    
    

with child as (
    select customer_id as from_field
    from "AdventureWorks2014"."gold"."gld_customer_metrics"
    where customer_id is not null
),

parent as (
    select customer_id as to_field
    from "AdventureWorks2014"."silver"."slvr_customers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


