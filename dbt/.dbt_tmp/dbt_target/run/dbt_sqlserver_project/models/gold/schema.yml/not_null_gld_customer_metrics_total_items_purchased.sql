
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_d0c46fdb592968b944e907ddb32848fc]
   as 
    
    



select total_items_purchased
from "AdventureWorks2014"."gold"."gld_customer_metrics"
where total_items_purchased is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_d0c46fdb592968b944e907ddb32848fc]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_d0c46fdb592968b944e907ddb32848fc]
  ;')