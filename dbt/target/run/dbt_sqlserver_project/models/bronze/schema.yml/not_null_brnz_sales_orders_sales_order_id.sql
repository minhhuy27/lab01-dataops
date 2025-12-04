
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_f34bc0ccea3efa4f021e98a79d785b72]
   as 
    
    



select sales_order_id
from "AdventureWorks2014"."bronze"."brnz_sales_orders"
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

    [dbt_test__audit.testview_f34bc0ccea3efa4f021e98a79d785b72]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_f34bc0ccea3efa4f021e98a79d785b72]
  ;')