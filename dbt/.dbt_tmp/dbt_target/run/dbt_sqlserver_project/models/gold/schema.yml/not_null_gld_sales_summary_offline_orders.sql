
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_46e39673b869a8368b61add7e024ff66]
   as 
    
    



select offline_orders
from "AdventureWorks2014"."gold"."gld_sales_summary"
where offline_orders is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_46e39673b869a8368b61add7e024ff66]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_46e39673b869a8368b61add7e024ff66]
  ;')