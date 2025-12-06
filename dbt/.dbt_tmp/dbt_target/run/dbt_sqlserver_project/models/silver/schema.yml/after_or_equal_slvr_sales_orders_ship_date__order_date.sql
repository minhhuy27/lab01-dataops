
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_ed0b17f4bf51035b2e4f10a1c52daec3]
   as 
    -- Fails when column is earlier than compare_column (both must be non-null)
    select *
    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    where ship_date is not null
      and order_date is not null
      and ship_date < order_date
;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_ed0b17f4bf51035b2e4f10a1c52daec3]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_ed0b17f4bf51035b2e4f10a1c52daec3]
  ;')