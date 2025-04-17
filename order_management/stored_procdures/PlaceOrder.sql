-- SQL Queries for Order Placement and Inventory Tracking
-- Process orders, deduct stock, calculate totals with discounts, handle multiple products

DELIMITER //

CREATE PROCEDURE PlaceOrder (
    IN p_customer_id INT,
    IN p_order_date DATE,
    IN p_products JSON
)
BEGIN
    -- Declare variables
    DECLARE v_order_id INT;
    DECLARE v_total_amount DECIMAL(10, 2);
    DECLARE v_product_id INT;
    DECLARE v_quantity INT;
    DECLARE v_unit_price DECIMAL(10, 2);
    DECLARE v_stock_available INT;
    DECLARE v_index INT DEFAULT 0;
    DECLARE v_array_length INT;

    -- Error handler: Rollback and raise error on SQL exception
    DECLARE exit handler FOR SQLEXCEPTION 
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error processing order';
    END;

    -- Start transaction to ensure all-or-nothing execution
    START TRANSACTION;

    -- Validate customer existence
    IF NOT EXISTS (SELECT 1 FROM Customers WHERE customer_id = p_customer_id) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid customer ID';
    END IF;

    -- Create a new order with initial total_amount as 0
    INSERT INTO Orders (customer_id, order_date, total_amount)
    VALUES (p_customer_id, p_order_date, 0);
    SET v_order_id = LAST_INSERT_ID();

    -- Get the number of products in the JSON array
    SET v_array_length = JSON_LENGTH(p_products);
    SET v_total_amount = 0;

    -- Iterate through the JSON product list
    WHILE v_index < v_array_length DO
        -- Extract product_id and quantity from JSON
        SET v_product_id = JSON_UNQUOTE(JSON_EXTRACT(p_products, CONCAT('$[', v_index, '].product_id')));
        SET v_quantity = JSON_UNQUOTE(JSON_EXTRACT(p_products, CONCAT('$[', v_index, '].quantity')));

        -- Validate quantity
        IF v_quantity <= 0 THEN
            ROLLBACK;
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Quantity must be positive';
        END IF;

        -- Fetch product price and stock, locking the row to avoid race conditions
        SELECT price, stock_quantity INTO v_unit_price, v_stock_available
        FROM Products
        WHERE product_id = v_product_id
        FOR UPDATE;
        
        -- Validate product existence
        IF v_unit_price IS NULL THEN
            ROLLBACK;
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid product ID';
        END IF;

        -- Check for sufficient stock
        IF v_stock_available < v_quantity THEN
            ROLLBACK;
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Insufficient stock for product';
        END IF;

        -- Insert order details for the current product
        INSERT INTO Order_Details (order_id, product_id, quantity, unit_price)
        VALUES (v_order_id, v_product_id, v_quantity, v_unit_price);

        -- Update product stock
        UPDATE Products
        SET stock_quantity = stock_quantity - v_quantity
        WHERE product_id = v_product_id;

        -- Accumulate total order amount
        SET v_total_amount = v_total_amount + (v_quantity * v_unit_price);

        -- Move to next product in the JSON array
        SET v_index = v_index + 1;
    END WHILE;

    -- Update the total amount in the order
    UPDATE Orders
    SET total_amount = v_total_amount
    WHERE order_id = v_order_id;

    -- Commit the transaction
    COMMIT;
END //

DELIMITER ;
