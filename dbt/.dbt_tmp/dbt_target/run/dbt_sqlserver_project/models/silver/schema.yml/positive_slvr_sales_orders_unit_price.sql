
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_889e789d198c18cc1f5982b91098544d]
   as 
    -- Fails when value is <= 0
    select *
    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    where unit_price <= 0
;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_889e789d198c18cc1f5982b91098544d]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_889e789d198c18cc1f5982b91098544d]
  ;')