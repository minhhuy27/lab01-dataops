
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_9b48a882fbecbf51efee95b115cedb90]
   as 
    -- Fails when value is outside [0,1]
    select *
    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    where unit_price_discount < 0
       or unit_price_discount > 1
;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_9b48a882fbecbf51efee95b115cedb90]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_9b48a882fbecbf51efee95b115cedb90]
  ;')