
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_d2f05f9de2b737dfae858c5053c7dd5a]
   as 
    
    



select sales_order_id
from "AdventureWorks2014"."silver"."slvr_sales_orders"
where sales_order_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_d2f05f9de2b737dfae858c5053c7dd5a]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_d2f05f9de2b737dfae858c5053c7dd5a]
  ;')