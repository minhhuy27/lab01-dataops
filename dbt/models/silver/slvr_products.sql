{{
    config(
        materialized='table'
    )
}}

with bronze_products as (
    select * from {{ ref('brnz_products') }}
),

cleaned as (
    select
        product_id,
        coalesce(product_name, 'Unknown') as product_name,
        product_number,
        make_flag,
        finished_goods_flag,
        coalesce(color, 'N/A') as color,
        standard_cost,
        list_price,
        coalesce(size, 'N/A') as size,
        coalesce(weight, 0) as weight,
        product_line,
        class,
        style,
        product_subcategory_id as subcategory_id,
        coalesce(subcategory_name, 'Uncategorized') as subcategory_name,
        product_category_id as category_id,
        sell_start_date,
        sell_end_date,
        discontinued_date,
        case 
            when discontinued_date is not null then 1
            else 0
        end as is_discontinued,
        case 
            when sell_end_date is null or sell_end_date >= getdate() then 1
            else 0
        end as is_active,
        list_price - standard_cost as margin_baseline,
        case 
            when list_price > 0 then (list_price - standard_cost) / list_price * 100
            else null
        end as margin_baseline_pct,
        last_modified_date
    from bronze_products
)

select * from cleaned
