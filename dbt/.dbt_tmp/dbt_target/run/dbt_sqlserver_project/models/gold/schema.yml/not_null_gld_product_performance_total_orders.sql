
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_66eee4cd0e2f303862e8fa73e5d53cfc]
   as 
    
    



select total_orders
from "AdventureWorks2014"."gold"."gld_product_performance"
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

    [dbt_test__audit.testview_66eee4cd0e2f303862e8fa73e5d53cfc]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_66eee4cd0e2f303862e8fa73e5d53cfc]
  ;')