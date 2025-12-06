
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_445a2c4593d431053dd42ea0a891e82d]
   as 
    
    

with all_values as (

    select
        order_channel as value_field,
        count(*) as n_records

    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    group by order_channel

)

select *
from all_values
where value_field not in (
    ''Online'',''Offline''
)


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_445a2c4593d431053dd42ea0a891e82d]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_445a2c4593d431053dd42ea0a891e82d]
  ;')