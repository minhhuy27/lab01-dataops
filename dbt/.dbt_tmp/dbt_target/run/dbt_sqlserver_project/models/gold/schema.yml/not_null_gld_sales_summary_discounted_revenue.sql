
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_f052c90439fd60925f047b25c51af548]
   as 
    
    



select discounted_revenue
from "AdventureWorks2014"."gold"."gld_sales_summary"
where discounted_revenue is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_f052c90439fd60925f047b25c51af548]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_f052c90439fd60925f047b25c51af548]
  ;')