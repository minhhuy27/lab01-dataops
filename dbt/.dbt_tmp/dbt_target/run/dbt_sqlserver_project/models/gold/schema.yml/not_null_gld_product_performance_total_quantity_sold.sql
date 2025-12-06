
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_71b343ec560ecce79ed4b6ab8f6037c4]
   as 
    
    



select total_quantity_sold
from "AdventureWorks2014"."gold"."gld_product_performance"
where total_quantity_sold is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_71b343ec560ecce79ed4b6ab8f6037c4]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_71b343ec560ecce79ed4b6ab8f6037c4]
  ;')