SELECT 
    line_items.order_item_key, 
    line_items.part_key, 
    line_items.line_number, 
    line_items.extended_price, 
    orders.order_key, 
    orders.customer_key, 
    orders.order_date, 
    {{discounted_amount('line_items.extended_price', 'line_items.discount_percentage')}} AS item_discount_amount 
FROM 
    {{ ref('stg_tpch_orders') }} AS orders 
JOIN 
    {{ ref('stg_tpch_line_items') }} AS line_items
ON orders.order_key = line_items.order_key 
