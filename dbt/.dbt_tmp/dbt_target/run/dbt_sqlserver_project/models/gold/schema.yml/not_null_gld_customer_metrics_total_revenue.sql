
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_62a14b0bf60fd566e30b0af772530cb1]
   as 
    
    



select total_revenue
from "AdventureWorks2014"."gold"."gld_customer_metrics"
where total_revenue is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_62a14b0bf60fd566e30b0af772530cb1]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_62a14b0bf60fd566e30b0af772530cb1]
  ;')