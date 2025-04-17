-- SQL Queries for Order Placement and Inventory Tracking
-- Process orders, deduct stock, calculate totals with discounts, handle multiple products

DELIMITER //

CREATE PROCEDURE PlaceOrder (
    IN p_customer_id INT,         -- Input: Customer placing the order
    IN p_order_date DATE,         -- Input: Date of the order
    IN p_products JSON            -- Input: JSON array of products [{product_id, quantity}, ...]
)
BEGIN
    -- Variable declarations
    DECLARE v_order_id INT;                   -- Holds the ID of the newly created order
    DECLARE v_total_amount DECIMAL(10, 2);    -- Total cost of the order
    DECLARE v_total_quantity INT;             -- Total quantity of all products in the order
    DECLARE v_product_id INT;                 -- Product ID currently being processed
    DECLARE v_quantity INT;                   -- Quantity of current product
    DECLARE v_unit_price DECIMAL(10, 2);      -- Unit price of the current product
    DECLARE v_discounted_price DECIMAL(10, 2);-- Discounted price after applying discount
    DECLARE v_discount_rate DECIMAL(4, 2);    -- Discount rate to apply (e.g., 0.05 for 5%)
    DECLARE v_stock_available INT;            -- Stock quantity available for the current product
    DECLARE v_index INT DEFAULT 0;            -- Index for iterating over JSON array
    DECLARE v_array_length INT;               -- Length of the JSON product array

    -- Error handler: rollback the transaction and raise a custom error
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error processing order';
    END;

    -- Start transaction to ensure all changes are atomic
    START TRANSACTION;

    -- Check if customer exists
    IF NOT EXISTS (SELECT 1 FROM Customers WHERE customer_id = p_customer_id) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid customer ID';
    END IF;

    -- Initialize product array processing
    SET v_array_length = JSON_LENGTH(p_products);  -- Get number of items in JSON array
    SET v_total_quantity = 0;
    SET v_index = 0;

    -- First loop: Validate each product and calculate total quantity
    WHILE v_index < v_array_length DO
        -- Extract product_id and quantity from JSON input
        SET v_product_id = JSON_UNQUOTE(JSON_EXTRACT(p_products, CONCAT('$[', v_index, '].product_id')));
        SET v_quantity = JSON_UNQUOTE(JSON_EXTRACT(p_products, CONCAT('$[', v_index, '].quantity')));

        -- Check for invalid quantity
        IF v_quantity <= 0 THEN
            ROLLBACK;
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Quantity must be positive';
        END IF;

        -- Lock product row and fetch unit price and stock
        SELECT price, stock_quantity INTO v_unit_price, v_stock_available
        FROM Products
        WHERE product_id = v_product_id
        FOR UPDATE;

        -- Check if product exists
        IF v_unit_price IS NULL THEN
            ROLLBACK;
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Invalid product ID: ', v_product_id);
        END IF;

        -- Check for sufficient stock
        IF v_stock_available < v_quantity THEN
            ROLLBACK;
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Insufficient stock for product ID: ', v_product_id);
        END IF;

        -- Accumulate total quantity
        SET v_total_quantity = v_total_quantity + v_quantity;

        -- Move to next product
        SET v_index = v_index + 1;
    END WHILE;

    -- Determine discount rate based on total quantity
    SET v_discount_rate = CASE
        WHEN v_total_quantity > 50 THEN 0.10  -- 10% discount for orders > 50 items
        WHEN v_total_quantity >= 10 THEN 0.05 -- 5% discount for orders >= 10 items
        ELSE 0.00                             -- No discount
    END;

    -- Insert the order (will update total_amount later)
    INSERT INTO Orders (customer_id, order_date, total_amount)
    VALUES (p_customer_id, p_order_date, 0);
    SET v_order_id = LAST_INSERT_ID(); -- Retrieve the auto-generated order ID

    -- Initialize variables for second loop
    SET v_total_amount = 0;
    SET v_index = 0;

    -- Second loop: Insert order details and update product stock
    WHILE v_index < v_array_length DO
        -- Re-extract product_id and quantity
        SET v_product_id = JSON_UNQUOTE(JSON_EXTRACT(p_products, CONCAT('$[', v_index, '].product_id')));
        SET v_quantity = JSON_UNQUOTE(JSON_EXTRACT(p_products, CONCAT('$[', v_index, '].quantity')));

        -- Fetch unit price again (already validated and locked earlier)
        SELECT price INTO v_unit_price
        FROM Products
        WHERE product_id = v_product_id;

        -- Apply discount
        SET v_discounted_price = v_unit_price * (1 - v_discount_rate);

        -- Insert order detail with discounted unit price
        INSERT INTO Order_Details (order_id, product_id, quantity, unit_price)
        VALUES (v_order_id, v_product_id, v_quantity, v_discounted_price);

        -- Update product stock
        UPDATE Products
        SET stock_quantity = stock_quantity - v_quantity
        WHERE product_id = v_product_id;

        -- Update order total amount
        SET v_total_amount = v_total_amount + (v_quantity * v_discounted_price);

        -- Move to next product
        SET v_index = v_index + 1;
    END WHILE;

    -- Update order record with the final total amount
    UPDATE Orders
    SET total_amount = v_total_amount
    WHERE order_id = v_order_id;

    -- Commit transaction
    COMMIT;
END //

DELIMITER ;
