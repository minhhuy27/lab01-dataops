
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_13d745e681540a7409ca84afacf04d0d]
   as 
    
    



select total_profit
from "AdventureWorks2014"."gold"."gld_product_performance"
where total_profit is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_13d745e681540a7409ca84afacf04d0d]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_13d745e681540a7409ca84afacf04d0d]
  ;')