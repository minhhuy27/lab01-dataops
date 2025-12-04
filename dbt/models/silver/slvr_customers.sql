{{
    config(
        materialized='table'
    )
}}

with bronze_customers as (
    select * from {{ ref('brnz_customers') }}
),

cleaned as (
    select
        customer_id,
        person_id,
        coalesce(first_name, 'Unknown') as first_name,
        coalesce(last_name, 'Unknown') as last_name,
        ltrim(rtrim(concat(coalesce(first_name, ''), ' ', coalesce(last_name, '')))) as full_name,
        email_promotion,
        case when email_promotion > 0 then 1 else 0 end as wants_email_promo,
        store_id,
        territory_id,
        case when store_id is not null then 'Store' else 'Individual' end as customer_type,
        case when person_id is not null then 1 else 0 end as has_person_record,
        last_modified_date
    from bronze_customers
)

select * from cleaned
