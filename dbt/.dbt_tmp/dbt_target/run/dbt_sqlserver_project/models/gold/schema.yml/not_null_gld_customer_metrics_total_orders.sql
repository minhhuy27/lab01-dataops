
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_78410b9b11c511714a2ba2783744cff1]
   as 
    
    



select total_orders
from "AdventureWorks2014"."gold"."gld_customer_metrics"
where total_orders is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_78410b9b11c511714a2ba2783744cff1]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_78410b9b11c511714a2ba2783744cff1]
  ;')