
---

# Inventory and Order Management System

## Overview

The **Inventory and Order Management System** is a database-driven solution designed for an e-commerce company to manage products, customers, orders, and inventory efficiently. It supports order placement, stock tracking, customer categorization, stock replenishment, and reporting. The system ensures data integrity, prevents stock shortages, applies bulk discounts, and provides insights into business operations.

This documentation covers the database schema, procedures, triggers, views, and usage instructions, aligning with the requirements across five phases:

1. **Database Design**: Schema for products, customers, orders, and inventory logs.
2. **Order Placement and Inventory**: Order processing and stock updates.
3. **Monitoring and Reporting**: Order summaries, stock alerts, and customer insights.
4. **Stock Replenishment and Automation**: Restocking and automated stock logging.
5. **Advanced Queries and Optimizations**: Views and performance enhancements.

Key features include:

- Order-wide discounts (5% for 10–50 items, 10% for &gt;50 items).
- Prevention of negative stock levels.
- Manual customer tier updates (Bronze, Silver, Gold). Will be automated in next version.
- Comprehensive inventory logging.

## System Components

### Database Schema

The system uses the `ecommerce_inventory` database with five tables, enforcing data integrity through constraints and relationships.

#### Tables

1. **Products**

   - **Purpose**: Stores product information.
   - **Columns**:
     - `product_id`: INT, auto-increment, primary key.
     - `name`: VARCHAR(100), not null.
     - `category`: VARCHAR(50), not null.
     - `price`: DECIMAL(10,2), not null, &gt;= 0.
     - `stock_quantity`: INT, not null, &gt;= 0.
     - `reorder_level`: INT, not null, &gt;= 0.
   - **Indexes**: `idx_stock_quantity` (on `stock_quantity`).

2. **Customers**

   - **Purpose**: Stores customer details and spending tiers.
   - **Columns**:
     - `customer_id`: INT, auto-increment, primary key.
     - `name`: VARCHAR(100), not null.
     - `email`: VARCHAR(100), not null, unique.
     - `phone`: VARCHAR(20), nullable.
     - `spending_tier`: ENUM('Bronze', 'Silver', 'Gold'), default 'Bronze'.
   - **Constraints**: `chk_email` (email contains '@' and '.').

3. **Orders**

   - **Purpose**: Tracks customer orders.
   - **Columns**:
     - `order_id`: INT, auto-increment, primary key.
     - `customer_id`: INT, not null, foreign key to `Customers(customer_id)`.
     - `order_date`: DATE, not null.
     - `total_amount`: DECIMAL(10,2), not null, &gt;= 0.
   - **Foreign Key**: `customer_id` (RESTRICT on delete, CASCADE on update).
   - **Indexes**: `idx_order_date`, `idx_total_amount`.

4. **Order_Details**

   - **Purpose**: Records products in each order.
   - **Columns**:
     - `order_detail_id`: INT, auto-increment, primary key.
     - `order_id`: INT, not null, foreign key to `Orders(order_id)`.
     - `product_id`: INT, not null, foreign key to `Products(product_id)`.
     - `quantity`: INT, not null, &gt; 0.
     - `unit_price`: DECIMAL(10,2), not null, &gt;= 0 (stores discounted price).
   - **Foreign Keys**: `order_id`, `product_id`
   - **Indexes**: `idx_order_id`, `idx_product_id`, `idx_quantity`.

5. **Inventory_Logs**

   - **Purpose**: Logs all stock changes (deductions, replenishments).
   - **Columns**:
     - `log_id`: INT, auto-increment, primary key.
     - `product_id`: INT, not null, foreign key to `Products(product_id)`.
     - `change_date`: TIMESTAMP, not null, default CURRENT_TIMESTAMP.
     - `quantity_changed`: INT, not null (negative for deductions, positive for additions).
     - `reason`: VARCHAR(100), not null (e.g., "Order #1", "Replenishment").
   - **Foreign Key**: `product_id`.
   - **Indexes**: `idx_change_date`.

### Procedures

1. **PlaceOrder**

   - **Purpose**: Processes customer orders, updates stock, applies discounts, and logs changes.
   - **Parameters**:
     - `p_customer_id`: INT, customer placing the order.
     - `p_order_date`: DATE, order date.
     - `p_products`: JSON, array of objects with `product_id` and `quantity`.
   - **Logic**:
     - Validates `p_customer_id` and product IDs.
     - Checks stock availability to prevent negative stock.
     - Calculates total order quantity.
     - Applies order-wide discount: 5% (10–50 items), 10% (&gt;50 items), 0% (&lt;10).
     - Inserts order into `Orders` and items into `Order_Details`.
     - Updates `Products.stock_quantity`.
     - Sets `Orders.total_amount` with discounted prices.
   - **Transaction**: Ensures atomicity; rolls back on errors.

2. **UpdateCustomerTiers**

   - **Purpose**: Categorizes customers into tiers based on spending (manual execution as at now, will be automated in next version).
   - **Logic**:
     - Computes `total_spending` per customer from `Orders.total_amount`.
     - Assigns tiers:
       - Gold: &gt;2000.
       - Silver: 500–2000.
       - Bronze: &lt;500.
     - Updates `Customers.spending_tier`.
   - **Execution**: Run manually (e.g., `CALL UpdateCustomerTiers();`).

3. **ReplenishStock**

   - **Purpose**: Restocks products below reorder level.
   - **Parameters**:
     - `p_product_id`: INT, specific product (NULL for all low-stock products).
     - `p_quantity`: INT, restock amount (NULL for default calculation).
   - **Logic**:
     - Identifies products where `stock_quantity < reorder_level`.
     - Restocks to `reorder_level - stock_quantity + 50` (if `p_quantity` is NULL) or `p_quantity`.
     - Updates `Products.stock_quantity`.
     - Logs additions in `Inventory_Logs` (reason: "Replenishment").

### Triggers

1. **after_order_detail_insert**
   - **Purpose**: Logs stock deductions after order items are added.
   - **Table**: `Order_Details`, `AFTER INSERT`.
   - **Logic**:
     - Inserts record into `Inventory_Logs`.
     - Fields: `product_id`, `change_date` (NOW()), `quantity_changed` (-quantity), `reason` ("Order #&lt;order_id&gt;").
   - **Ensures**: Every order item reduces stock and logs the change automatically.

### Views

1. **vw_order_summary**

   - **Purpose**: Summarizes orders for easy access.
   - **Columns**:
     - `order_id`, `order_date`, `customer_id`, `customer_name`, `total_amount`, `number_of_items`, `total_quantity`.
   - **Joins**: `Orders`, `Customers`, `Order_Details`.
   - **Order**: `order_date DESC`.

2. **vw_customer_tiers**

   - **Purpose**: Shows customer spending and tiers.
   - **Columns**:
     - `customer_id`, `customer_name`, `total_spending`, `total_discount_savings`, `spending_tier`.
   - **Logic**:
     - `total_spending`: SUM(`Orders.total_amount`).
     - `total_discount_savings`: SUM(`quantity * price`) - SUM(`total_amount`).
     - Includes customers with no orders (0 spending/savings).
   - **Order**: `total_spending DESC`.

3. **vw_inventory_changes**

   - **Purpose**: Displays inventory change history.
   - **Columns**:
     - `log_id`, `product_id`, `product_name`, `change_date`, `quantity_changed`, `reason`.
   - **Joins**: `Inventory_Logs`, `Products`.
   - **Order**: `change_date DESC`.


4. **vw_stock_insights**

   - **Purpose**: Analyzes stock status and sales.
   - **Columns**:
     - `product_id`, `product_name`, `stock_quantity`, `reorder_level`, `stock_status`, `total_quantity_sold`.
   - **Logic**:
     - `stock_status`: "Low Stock" if `stock_quantity < reorder_level`, else "Sufficient".
     - `total_quantity_sold`: SUM(`Order_Details.quantity`).
   - **Order**: `stock_quantity ASC`.

## Usage Instructions

### Setup

1. **Create Database**:

   ```sql
   CREATE DATABASE ecommerce_inventory;
   USE ecommerce_inventory;
   ```

2. **Run Schema**: Execute the table creation scripts (`Products`, `Customers`, `Orders`, `Order_Details`, `Inventory_Logs`).

3. **Add Procedures**: Create `PlaceOrder`, `UpdateCustomerTiers`, `ReplenishStock`.

4. **Add Trigger**: Create `after_order_detail_insert`.

5. **Add Views**: Create `vw_order_summary`,  `vw_customer_tiers`, `vw_inventory_changes`, `vw_stock_insights`.

6. **Insert Test Data**:

   ```sql
   -- Customers: 3 for tier testing
   INSERT INTO Customers (name, email) VALUES
   ('John Doe', 'john.doe@example.com'),
   ('Jane Smith', 'jane.smith@example.com'),
   ('Alice Johnson', 'alice.johnson@example.com');
   
   -- Products: 3 for stock and discount testing
   INSERT INTO Products (name, category, price, stock_quantity, reorder_level) VALUES
   ('Wireless Mouse', 'Electronics', 29.99, 100, 20),
   ('USB-C Cable', 'Electronics', 9.99, 200, 50),
   ('Laptop Stand', 'Accessories', 49.99, 50, 10);
   ```

### Operations

1. **Place an Order**:

   ```sql
   CALL PlaceOrder(1, '2025-04-16', '[{"product_id": 1, "quantity": 5}, {"product_id": 2, "quantity": 3}]');
   ```

   - Validates inputs, applies discounts, updates stock, logs deductions.
   - JSON format: Array of `{product_id: INT, quantity: INT}` makes it able to place multiple orders.

2. **Update Customer Tiers**:

   ```sql
   CALL UpdateCustomerTiers();
   ```

   - Manually run after orders to update `spending_tier` based on `total_spending`. Will be updated 
     soon to make it automated.

3. **Replenish Stock**:

   ```sql
   CALL ReplenishStock(3, NULL); -- Default restock for product_id 3
   CALL ReplenishStock(NULL, 100); -- Restock 100 for all low-stock products
   ```

   - Restocks low products, logs changes.

4. **View Summaries**:

   - Orders: `SELECT * FROM vw_order_summary;`
   - Customers: `SELECT * FROM vw_customer_tiers;`
   - Inventory Changes: `SELECT * FROM vw_inventory_changes;`
   - Reports: `SELECT * FROM vw_order_report; SELECT * FROM vw_stock_insights;`

### Sample Outputs

**Setup**:

```sql
CALL PlaceOrder(1, '2025-04-16', '[{"product_id": 1, "quantity": 5}, {"product_id": 2, "quantity": 3}]');
CALL PlaceOrder(2, '2025-04-16', '[{"product_id": 1, "quantity": 10}, {"product_id": 2, "quantity": 5}]');
CALL PlaceOrder(2, '2025-04-16', '[{"product_id": 2, "quantity": 40}, {"product_id": 3, "quantity": 20}]');
CALL UpdateCustomerTiers();
CALL ReplenishStock(3, NULL);
```

1. **Orders**:

   ```sql
   SELECT * FROM Orders;
   ```

   ```
   order_id | customer_id | order_date  | total_amount
   ---------|------------|-------------|--------------
   1        | 1          | 2025-04-16  | 179.92
   2        | 2          | 2025-04-16  | 332.37
   3        | 2          | 2025-04-16  | 1259.40
   ```

2. **Customer Tiers**:

   ```sql
   SELECT * FROM vw_customer_tiers;
   ```

   ```
   customer_id | customer_name   | total_spending || spending_tier
   ------------|-----------------|----------------|----------------
   2           | Jane Smith      | 1591.77        | Silver
   1           | John Doe        | 179.92         | Bronze
   3           | Alice Johnson   | 0.00           | Bronze
   ```

3. **Inventory Changes**:

   ```sql
   SELECT * FROM vw_inventory_changes LIMIT 4;
   ```

   ```
   log_id | product_id | product_name     | change_date         | quantity_changed | reason
   -------|------------|------------------|---------------------|------------------|-----------
   7      | 3          | Laptop Stand     | 2025-04-16 ...      | 20               | Replenishment
   6      | 3          | Laptop Stand     | 2025-04-16 ...      | -20              | Order #3

   ```

   ```

4. **Stock Insights**:

   ```sql
   SELECT * FROM vw_stock_insights;
   ```

   ```
   product_id | product_name     | stock_quantity | reorder_level | stock_status | total_quantity_sold
   -----------|------------------|----------------|---------------|--------------|---------------------
   3          | Laptop Stand     | 50             | 10            | Sufficient   | 20
   1          | Wireless Mouse   | 85             | 20            | Sufficient   | 15
   ```

## Optimizations

- **Indexes**:
  - `idx_stock_quantity`, `idx_order_date`, `idx_total_amount`, `idx_order_id`, `idx_product_id`, `idx_quantity`, `idx_change_date` speed up joins and filters.
- **Transactions**:
  - `PlaceOrder`, `UpdateCustomerTiers`, `ReplenishStock` use transactions for consistency.
- **Locks**:
  - `FOR UPDATE` in `PlaceOrder` prevents stock race conditions.
- **Views**:
  - Use `LEFT JOIN` for robustness, `COALESCE` for null handling.
- **Constraints**:
  - Foreign keys, checks (`price >= 0`, `quantity > 0`) ensure integrity.
- **Manual Tiers**:
  - `UpdateCustomerTiers` runs on demand, reducing overhead for small datasets.
