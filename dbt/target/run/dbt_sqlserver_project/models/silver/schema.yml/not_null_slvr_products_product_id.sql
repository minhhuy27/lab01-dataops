
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_4747618344125108fc15657b190b9ae1]
   as 
    
    



select product_id
from "AdventureWorks2014"."silver"."slvr_products"
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

    [dbt_test__audit.testview_4747618344125108fc15657b190b9ae1]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_4747618344125108fc15657b190b9ae1]
  ;')