
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_bc56b099d2c134c467cc6d9a9814f225]
   as 
    
    

with child as (
    select product_id as from_field
    from "AdventureWorks2014"."silver"."slvr_sales_orders"
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


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_bc56b099d2c134c467cc6d9a9814f225]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_bc56b099d2c134c467cc6d9a9814f225]
  ;')