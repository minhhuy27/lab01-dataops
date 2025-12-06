
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_eeb58e705bb439bf4c643cb92c3a516a]
   as 
    
    



select customer_id
from "AdventureWorks2014"."bronze"."brnz_customers"
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

    [dbt_test__audit.testview_eeb58e705bb439bf4c643cb92c3a516a]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_eeb58e705bb439bf4c643cb92c3a516a]
  ;')