
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_9cca0df3a2130b8ab386fbb4202eaa10]
   as 
    
    



select orders_with_discount
from "AdventureWorks2014"."gold"."gld_customer_metrics"
where orders_with_discount is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_9cca0df3a2130b8ab386fbb4202eaa10]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_9cca0df3a2130b8ab386fbb4202eaa10]
  ;')