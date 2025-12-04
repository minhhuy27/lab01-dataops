{{
    config(
        materialized='table'
    )
}}

with sales as (
    select * from {{ ref('brnz_sales_orders') }}
),

customers as (
    select
        customer_id,
        first_name,
        last_name,
        territory_id as customer_territory_id
    from {{ ref('brnz_customers') }}
),

products as (
    select
        product_id,
        product_name,
        subcategory_name,
        product_category_id,
        standard_cost,
        list_price
    from {{ ref('brnz_products') }}
),

enriched as (
    select
        s.sales_order_id,
        s.order_detail_id,
        s.order_date,
        s.due_date,
        s.ship_date,
        s.status,
        case 
            when s.online_order_flag = 1 then 'Online'
            else 'Offline'
        end as order_channel,
        s.sales_order_number,
        s.purchase_order_number,
        s.customer_id,
        c.first_name,
        c.last_name,
        ltrim(rtrim(concat(coalesce(c.first_name, ''), ' ', coalesce(c.last_name, '')))) as customer_name,
        s.sales_person_id,
        coalesce(c.customer_territory_id, s.territory_id) as territory_id,
        s.product_id,
        p.product_name,
        p.subcategory_name,
        p.product_category_id,
        s.order_qty,
        s.unit_price,
        s.unit_price_discount,
        s.line_total,
        -- Calculated fields
        s.unit_price * s.order_qty as gross_amount,
        s.line_total / nullif(s.order_qty, 0) as effective_unit_price,
        case 
            when s.unit_price_discount > 0 then 1
            else 0
        end as has_discount,
        case 
            when p.standard_cost is not null then s.unit_price - p.standard_cost
            else null
        end as unit_margin,
        case 
            when p.standard_cost is not null and s.unit_price > 0 then 
                (s.unit_price - p.standard_cost) / s.unit_price * 100
            else null
        end as unit_margin_pct,
        s.last_modified_date
    from sales s
    left join customers c
        on s.customer_id = c.customer_id
    left join products p
        on s.product_id = p.product_id
    where s.order_qty > 0
        and s.unit_price >= 0
)

select * from enriched
