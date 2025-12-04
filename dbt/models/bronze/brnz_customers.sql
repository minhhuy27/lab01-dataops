{{
    config(
        materialized='view'
    )
}}

with customer as (
    select * from {{ source('adventureworks', 'Customer') }}
),

person as (
    select * from {{ source('adventureworks_person', 'Person') }}
),

staged as (
    select
        c.CustomerID as customer_id,
        c.PersonID as person_id,
        nullif(ltrim(rtrim(p.FirstName)), '') as first_name,
        nullif(ltrim(rtrim(p.LastName)), '') as last_name,
        p.EmailPromotion as email_promotion,
        c.StoreID as store_id,
        c.TerritoryID as territory_id,
        c.ModifiedDate as last_modified_date
    from customer c
    left join person p
        on c.PersonID = p.BusinessEntityID
)

select * from staged 
