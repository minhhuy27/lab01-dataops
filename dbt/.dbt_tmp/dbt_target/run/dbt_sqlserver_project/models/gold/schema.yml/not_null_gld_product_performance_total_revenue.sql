
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_b797556c367d2e4e42d2696561e65928]
   as 
    
    



select total_revenue
from "AdventureWorks2014"."gold"."gld_product_performance"
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

    [dbt_test__audit.testview_b797556c367d2e4e42d2696561e65928]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_b797556c367d2e4e42d2696561e65928]
  ;')