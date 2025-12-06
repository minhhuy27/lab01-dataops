
  
    USE [AdventureWorks2014];
    USE [AdventureWorks2014];
    
    

    

    
    USE [AdventureWorks2014];
    EXEC('
        create view "gold"."gld_product_performance__dbt_tmp__dbt_tmp_vw" as 

with products as (
    select * from "AdventureWorks2014"."silver"."slvr_products"
),

sales as (
    select * from "AdventureWorks2014"."silver"."slvr_sales_orders"
),

product_sales as (
    select
        p.product_id,
        p.product_name,
        p.subcategory_name,
        p.color,
        p.list_price,
        p.standard_cost,
        count(distinct s.sales_order_id) as total_orders,
        coalesce(sum(s.order_qty), 0) as total_quantity_sold,
        coalesce(sum(s.line_total), 0) as total_revenue,
        coalesce(avg(s.unit_price), 0) as avg_selling_price,
        coalesce(sum(s.line_total), 0) - (coalesce(sum(s.order_qty), 0) * coalesce(p.standard_cost, 0)) as total_profit,
        case 
            when coalesce(sum(s.line_total), 0) > 0 then 
                (coalesce(sum(s.line_total), 0) - (coalesce(sum(s.order_qty), 0) * coalesce(p.standard_cost, 0))) / coalesce(sum(s.line_total), 0) * 100
            else 0
        end as profit_margin_pct
    from products p
    left join sales s
        on p.product_id = s.product_id
    group by 
        p.product_id, 
        p.product_name, 
        p.subcategory_name, 
        p.color, 
        p.list_price, 
        p.standard_cost
)

select * from product_sales;
    ')

EXEC('
            SELECT * INTO "AdventureWorks2014"."gold"."gld_product_performance__dbt_tmp" FROM "AdventureWorks2014"."gold"."gld_product_performance__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS gold.gld_product_performance__dbt_tmp__dbt_tmp_vw')



    
    use [AdventureWorks2014];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'gold_gld_product_performance__dbt_tmp_cci'
        AND object_id=object_id('gold_gld_product_performance__dbt_tmp')
    )
    DROP index "gold"."gld_product_performance__dbt_tmp".gold_gld_product_performance__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX gold_gld_product_performance__dbt_tmp_cci
    ON "gold"."gld_product_performance__dbt_tmp"

   


  