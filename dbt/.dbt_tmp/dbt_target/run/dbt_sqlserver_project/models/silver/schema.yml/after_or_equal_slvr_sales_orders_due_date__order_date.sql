
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_0b2458a30e46ee4f0ab1b1d51d257e07]
   as 
    -- Fails when column is earlier than compare_column (both must be non-null)
    select *
    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    where due_date is not null
      and order_date is not null
      and due_date < order_date
;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_0b2458a30e46ee4f0ab1b1d51d257e07]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_0b2458a30e46ee4f0ab1b1d51d257e07]
  ;')