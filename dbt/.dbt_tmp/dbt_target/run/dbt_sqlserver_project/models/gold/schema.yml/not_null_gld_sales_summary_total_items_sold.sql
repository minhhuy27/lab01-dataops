
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_b1f43df1bb8e819930de757cae3a8728]
   as 
    
    



select total_items_sold
from "AdventureWorks2014"."gold"."gld_sales_summary"
where total_items_sold is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_b1f43df1bb8e819930de757cae3a8728]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_b1f43df1bb8e819930de757cae3a8728]
  ;')