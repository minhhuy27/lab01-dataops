
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_519f26ab3181bf1998e2877231c0fc8e]
   as 
    
    



select order_detail_id
from "AdventureWorks2014"."bronze"."brnz_sales_orders"
where order_detail_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_519f26ab3181bf1998e2877231c0fc8e]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_519f26ab3181bf1998e2877231c0fc8e]
  ;')