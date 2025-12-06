
    -- Fails when value is <= 0
    select *
    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    where order_qty <= 0
