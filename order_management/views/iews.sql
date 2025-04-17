
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

-- View: Low Stock
-- Lists products that are below their reorder level, including the quantity needed to restock.
CREATE VIEW vw_low_stock AS
SELECT 
    product_id,
    name AS product_name,
    category,
    stock_quantity,
    reorder_level,
    (reorder_level - stock_quantity) AS restock_needed
FROM Products
WHERE stock_quantity < reorder_level
ORDER BY stock_quantity ASC;

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