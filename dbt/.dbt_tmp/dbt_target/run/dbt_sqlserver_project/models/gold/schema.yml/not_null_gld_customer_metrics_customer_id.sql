
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_127db5d1b4b9c98e0c08ea229f43d7b6]
   as 
    
    



select customer_id
from "AdventureWorks2014"."gold"."gld_customer_metrics"
where customer_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_127db5d1b4b9c98e0c08ea229f43d7b6]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_127db5d1b4b9c98e0c08ea229f43d7b6]
  ;')