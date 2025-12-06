
  
  

  
  USE [AdventureWorks2014];
  EXEC('create view 

    [dbt_test__audit.testview_d186db4a9ababa2e52e4a652a98bde96]
   as 
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    group by status

)

select *
from all_values
where value_field not in (
    ''1'',''2'',''3'',''4'',''5'',''6''
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

    [dbt_test__audit.testview_d186db4a9ababa2e52e4a652a98bde96]
  
  ) dbt_internal_test;

  USE [AdventureWorks2014];
  EXEC('drop view 

    [dbt_test__audit.testview_d186db4a9ababa2e52e4a652a98bde96]
  ;')