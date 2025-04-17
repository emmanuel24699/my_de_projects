DELIMITER //

-- Procedure: ReplenishStock
-- Replenishes stock for a specific product or all low-stock products with optional quantity override

CREATE PROCEDURE ReplenishStock (
    IN p_product_id INT,         -- Optional: specific product to replenish (NULL = all)
    IN p_quantity INT            -- Optional: specific quantity to add (NULL = auto-calculate)
)
BEGIN
    -- Buffer quantity to restock above the reorder level when auto-calculating
    DECLARE v_buffer INT DEFAULT 50;

    -- Variables for cursor iteration
    DECLARE v_product_id INT;
    DECLARE v_restock_needed INT;
    DECLARE v_restock_quantity INT;
    DECLARE done INT DEFAULT FALSE;

    -- Cursor to select products needing restocking
    DECLARE cur CURSOR FOR 
        SELECT 
            product_id,
            (reorder_level - stock_quantity) AS restock_needed
        FROM Products
        WHERE stock_quantity < reorder_level
        AND (p_product_id IS NULL OR product_id = p_product_id);

    -- Handler for end of cursor
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Handler for any SQL exception during transaction
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error during replenishment';
    END;

    START TRANSACTION;

    -- Validate product ID input if provided
    IF p_product_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Products WHERE product_id = p_product_id) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid product ID';
    END IF;

    -- Validate quantity input if provided
    IF p_quantity IS NOT NULL AND p_quantity <= 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Restock quantity must be positive';
    END IF;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_product_id, v_restock_needed;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Calculate how much to restock (either user-defined or auto)
        SET v_restock_quantity = IF(p_quantity IS NULL, v_restock_needed + v_buffer, p_quantity);

        -- Update product stock
        UPDATE Products
        SET stock_quantity = stock_quantity + v_restock_quantity
        WHERE product_id = v_product_id;

        -- Log the stock change
        INSERT INTO Inventory_Logs (product_id, change_date, quantity_changed, reason)
        VALUES (
            v_product_id,
            NOW(),
            v_restock_quantity,
            'Replenishment'
        );
    END LOOP;
    CLOSE cur;

    -- Check if any replenishment occurred (use 1-second grace for logging timestamp)
    IF NOT EXISTS (
        SELECT 1 
        FROM Inventory_Logs 
        WHERE reason = 'Replenishment' 
        AND change_date >= NOW() - INTERVAL 1 SECOND
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No products needed replenishment';
    END IF;

    COMMIT;
END //

DELIMITER ;