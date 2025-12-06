
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_a7d5b93e69c3a86476d2127d37390552]
   as 
    
    



select online_orders
from "AdventureWorks2014"."gold"."gld_sales_summary"
where online_orders is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_a7d5b93e69c3a86476d2127d37390552]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_a7d5b93e69c3a86476d2127d37390552]
  ;')