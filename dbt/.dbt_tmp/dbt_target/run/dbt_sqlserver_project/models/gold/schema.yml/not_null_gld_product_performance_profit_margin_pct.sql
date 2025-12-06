
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_e2ea9b04fa85a59620bffa79552939e8]
   as 
    
    



select profit_margin_pct
from "AdventureWorks2014"."gold"."gld_product_performance"
where profit_margin_pct is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_e2ea9b04fa85a59620bffa79552939e8]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_e2ea9b04fa85a59620bffa79552939e8]
  ;')