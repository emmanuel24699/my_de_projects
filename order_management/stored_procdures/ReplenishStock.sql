DELIMITER //

-- Procedure: ReplenishStock
-- Replenishes stock for a specific product or all low-stock products with optional quantity override

DELIMITER //

CREATE PROCEDURE ReplenishStock (
    IN p_product_id INT, 
    IN p_quantity INT 
)
BEGIN
    DECLARE v_buffer INT DEFAULT 50;
    DECLARE v_product_id INT;
    DECLARE v_restock_needed INT;
    DECLARE v_restock_quantity INT;
    DECLARE v_updated INT DEFAULT 0;
    DECLARE done INT DEFAULT FALSE;
    DECLARE cur CURSOR FOR 
        SELECT 
            product_id,
            (reorder_level - stock_quantity) AS restock_needed
        FROM Products
        WHERE stock_quantity < reorder_level
        AND (p_product_id IS NULL OR product_id = p_product_id);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error during replenishment';
    END;

    START TRANSACTION;

    -- Validate inputs
    IF p_product_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Products WHERE product_id = p_product_id) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid product ID';
    END IF;

    IF p_quantity IS NOT NULL AND p_quantity <= 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Restock quantity must be positive';
    END IF;

    -- Process low-stock products
    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_product_id, v_restock_needed;
        IF done THEN
            LEAVE read_loop;
        END IF;

        SET v_restock_quantity = IF(p_quantity IS NULL, v_restock_needed + v_buffer, p_quantity);

        UPDATE Products
        SET stock_quantity = stock_quantity + v_restock_quantity
        WHERE product_id = v_product_id;

        INSERT INTO Inventory_Logs (product_id, change_date, quantity_changed, reason)
        VALUES (
            v_product_id,
            NOW(),
            v_restock_quantity,
            'Replenishment'
        );

        SET v_updated = v_updated + 1;
    END LOOP;
    CLOSE cur;

    COMMIT;

    -- Notify if no product requires update
    IF v_updated = 0 THEN
        SELECT 'No products needed replenishment' AS message;
    END IF;
END //

DELIMITER ;