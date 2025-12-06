
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_4b16c0c0e13c2409a70c45d510dee434]
   as 
    
    

with child as (
    select customer_id as from_field
    from "AdventureWorks2014"."gold"."gld_customer_metrics"
    where customer_id is not null
),

parent as (
    select customer_id as to_field
    from "AdventureWorks2014"."silver"."slvr_customers"
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

    [dbt_test__audit.testview_4b16c0c0e13c2409a70c45d510dee434]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_4b16c0c0e13c2409a70c45d510dee434]
  ;')