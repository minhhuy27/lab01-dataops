
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_60e86faff5b5613b910501ca898fe93b]
   as 
    
    



select product_id
from "AdventureWorks2014"."bronze"."brnz_products"
where product_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_60e86faff5b5613b910501ca898fe93b]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_60e86faff5b5613b910501ca898fe93b]
  ;')