
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_78ae9bed65a225ee6eb5a4579b1dbd68]
   as 
    
    

with child as (
    select customer_id as from_field
    from "AdventureWorks2014"."bronze"."brnz_sales_orders"
    where customer_id is not null
),

parent as (
    select customer_id as to_field
    from "AdventureWorks2014"."bronze"."brnz_customers"
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

    [dbt_test__audit.testview_78ae9bed65a225ee6eb5a4579b1dbd68]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_78ae9bed65a225ee6eb5a4579b1dbd68]
  ;')