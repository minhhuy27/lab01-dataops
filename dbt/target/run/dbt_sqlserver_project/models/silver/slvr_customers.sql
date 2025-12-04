
  
    USE [AdventureWorks2014];
    USE [AdventureWorks2014];
    
    

    

    
    USE [AdventureWorks2014];
    EXEC('
        create view "silver"."slvr_customers__dbt_tmp__dbt_tmp_vw" as 

with bronze_customers as (
    select * from "AdventureWorks2014"."bronze"."brnz_customers"
),

cleaned as (
    select
        customer_id,
        person_id,
        coalesce(first_name, ''Unknown'') as first_name,
        coalesce(last_name, ''Unknown'') as last_name,
        ltrim(rtrim(concat(coalesce(first_name, ''''), '' '', coalesce(last_name, '''')))) as full_name,
        email_promotion,
        case when email_promotion > 0 then 1 else 0 end as wants_email_promo,
        store_id,
        territory_id,
        case when store_id is not null then ''Store'' else ''Individual'' end as customer_type,
        case when person_id is not null then 1 else 0 end as has_person_record,
        last_modified_date
    from bronze_customers
)

select * from cleaned;
    ')

EXEC('
            SELECT * INTO "AdventureWorks2014"."silver"."slvr_customers__dbt_tmp" FROM "AdventureWorks2014"."silver"."slvr_customers__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS silver.slvr_customers__dbt_tmp__dbt_tmp_vw')



    
    use [AdventureWorks2014];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'silver_slvr_customers__dbt_tmp_cci'
        AND object_id=object_id('silver_slvr_customers__dbt_tmp')
    )
    DROP index "silver"."slvr_customers__dbt_tmp".silver_slvr_customers__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX silver_slvr_customers__dbt_tmp_cci
    ON "silver"."slvr_customers__dbt_tmp"

   


  