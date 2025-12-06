
    -- Fails when column is earlier than compare_column (both must be non-null)
    select *
    from "AdventureWorks2014"."silver"."slvr_sales_orders"
    where ship_date is not null
      and order_date is not null
      and ship_date < order_date
