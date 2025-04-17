
-- View: Order Summary
-- Provides a summary of each order, including order details, customer info, number of items, and total quantity ordered.

CREATE VIEW vw_order_summary AS
SELECT 
    o.order_id,
    o.order_date,
    c.customer_id,
    c.name AS customer_name,
    o.total_amount,
    COUNT(od.order_detail_id) AS number_of_items,
    SUM(od.quantity) AS total_quantity
FROM Orders o
JOIN Customers c ON o.customer_id = c.customer_id
LEFT JOIN Order_Details od ON o.order_id = od.order_id
GROUP BY o.order_id, o.order_date, c.customer_id, c.name, o.total_amount
ORDER BY o.order_date DESC;

-- View: Stock Insights
-- Provides insights into product stock levels, including stock status and total quantity sold.
CREATE VIEW vw_stock_insights AS
SELECT 
    p.product_id,
    p.name AS product_name,
    p.stock_quantity,
    p.reorder_level,
    CASE 
        WHEN p.stock_quantity < p.reorder_level THEN 'Low Stock'
        ELSE 'Sufficient'
    END AS stock_status,
    COALESCE(SUM(od.quantity), 0) AS total_quantity_sold
FROM Products p
LEFT JOIN Order_Details od ON p.product_id = od.product_id
GROUP BY p.product_id, p.name, p.stock_quantity, p.reorder_level
ORDER BY p.stock_quantity ASC;


-- View: Customer Tiers
-- Categorizes customers based on their total spending, providing insights into customer loyalty and spending behavior.
CREATE VIEW vw_customer_tiers AS
SELECT 
    c.customer_id,
    c.name AS customer_name,
    COALESCE(SUM(o.total_amount), 0) AS total_spending,
    c.spending_tier
FROM Customers c
LEFT JOIN Orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.spending_tier
ORDER BY total_spending DESC;

-- view: vw_inventory_changes
-- Provides a log of inventory changes, including product details and reasons for changes.
CREATE VIEW vw_inventory_changes AS
SELECT 
    il.log_id,
    il.product_id,
    p.name AS product_name,
    il.change_date,
    il.quantity_changed,
    il.reason
FROM Inventory_Logs il
JOIN Products p ON il.product_id = p.product_id
ORDER BY il.change_date DESC;