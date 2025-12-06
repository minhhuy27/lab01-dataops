
    
    

with child as (
    select product_id as from_field
    from "AdventureWorks2014"."gold"."gld_product_performance"
    where product_id is not null
),

parent as (
    select product_id as to_field
    from "AdventureWorks2014"."silver"."slvr_products"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


