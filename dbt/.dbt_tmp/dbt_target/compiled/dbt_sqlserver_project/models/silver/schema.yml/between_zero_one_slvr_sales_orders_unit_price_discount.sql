
    -- Fails when value is outside [0,1]
    select *
    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    where unit_price_discount < 0
       or unit_price_discount > 1
