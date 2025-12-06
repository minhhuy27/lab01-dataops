
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_bfc07dfc83027e07c55fb3f3c1b14093]
   as 
    
    

with child as (
    select product_id as from_field
    from "AdventureWorks2014"."gold"."gld_product_performance"
    where product_id is not null
),

parent as (
    select product_id as to_field
    from "AdventureWorks2014"."silver"."slvr_products"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_bfc07dfc83027e07c55fb3f3c1b14093]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_bfc07dfc83027e07c55fb3f3c1b14093]
  ;')