with product as (
    select * from {{ source('adventureworks_production', 'Product') }}
),

product_subcategory as (
    select * from {{ source('adventureworks_production', 'ProductSubcategory') }}
),

staged as (
    select
        p.ProductID as product_id,
        nullif(ltrim(rtrim(p.Name)), '') as product_name,
        p.ProductNumber as product_number,
        p.MakeFlag as make_flag,
        p.FinishedGoodsFlag as finished_goods_flag,
        nullif(ltrim(rtrim(p.Color)), '') as color,
        p.SafetyStockLevel as safety_stock_level,
        p.ReorderPoint as reorder_point,
        p.StandardCost as standard_cost,
        p.ListPrice as list_price,
        nullif(ltrim(rtrim(p.Size)), '') as size,
        p.SizeUnitMeasureCode as size_unit_measure_code,
        p.WeightUnitMeasureCode as weight_unit_measure_code,
        p.Weight as weight,
        p.DaysToManufacture as days_to_manufacture,
        nullif(ltrim(rtrim(p.ProductLine)), '') as product_line,
        nullif(ltrim(rtrim(p.Class)), '') as class,
        nullif(ltrim(rtrim(p.Style)), '') as style,
        p.ProductSubcategoryID as product_subcategory_id,
        p.ProductModelID as product_model_id,
        p.SellStartDate as sell_start_date,
        p.SellEndDate as sell_end_date,
        p.DiscontinuedDate as discontinued_date,
        ps.Name as subcategory_name,
        ps.ProductCategoryID as product_category_id,
        p.ModifiedDate as last_modified_date
    from product p
    left join product_subcategory ps
        on p.ProductSubcategoryID = ps.ProductSubcategoryID
)

select * from staged 
