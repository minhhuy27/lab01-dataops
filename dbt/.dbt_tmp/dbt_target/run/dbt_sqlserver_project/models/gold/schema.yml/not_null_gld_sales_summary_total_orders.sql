
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_fa230c5939970e5e1f7a4271353a4217]
   as 
    
    



select total_orders
from "AdventureWorks2014"."gold"."gld_sales_summary"
where total_orders is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_fa230c5939970e5e1f7a4271353a4217]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_fa230c5939970e5e1f7a4271353a4217]
  ;')