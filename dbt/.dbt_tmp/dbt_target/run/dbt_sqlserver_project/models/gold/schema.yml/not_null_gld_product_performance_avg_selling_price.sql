
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_a7c993423067d12a0c6b08f7cea3a309]
   as 
    
    



select avg_selling_price
from "AdventureWorks2014"."gold"."gld_product_performance"
where avg_selling_price is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_a7c993423067d12a0c6b08f7cea3a309]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_a7c993423067d12a0c6b08f7cea3a309]
  ;')