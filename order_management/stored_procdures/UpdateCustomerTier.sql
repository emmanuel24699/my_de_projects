DELIMITER //

-- Procedure: UpdateCustomerTiers
-- Updates the spending tier of each customer based on their total order spending.
--              Tiers are assigned as follows:
--              - Gold: Spending > 2000
--              - Silver: Spending >= 500
--              - Bronze: Spending < 500

CREATE PROCEDURE UpdateCustomerTiers ()
BEGIN
    -- Variable declarations
    DECLARE v_customer_id INT;
    DECLARE v_total_spending DECIMAL(10, 2);
    DECLARE done INT DEFAULT FALSE;

    -- Cursor declaration to retrieve each customer and their total spending
    DECLARE cur CURSOR FOR 
        SELECT 
            c.customer_id,
            COALESCE(SUM(o.total_amount), 0) AS total_spending
        FROM Customers c
        LEFT JOIN Orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id;

    -- Handler to mark end of data for the cursor
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Error handler to rollback transaction and raise an error if any SQL exception occurs
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error updating customer tiers';
    END;

    -- Begin transaction to ensure updates are atomic
    START TRANSACTION;

    -- Open the cursor for processing
    OPEN cur;

    -- Loop through each customer from the cursor
    read_loop: LOOP
        FETCH cur INTO v_customer_id, v_total_spending;

        -- Exit loop if no more rows
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Update the spending tier of the current customer based on total spending
        UPDATE Customers
        SET spending_tier = CASE 
            WHEN v_total_spending > 2000 THEN 'Gold'
            WHEN v_total_spending >= 500 THEN 'Silver'
            ELSE 'Bronze'
        END
        WHERE customer_id = v_customer_id;
    END LOOP;

    -- Close the cursor after processing all customers
    CLOSE cur;

    -- Commit the transaction
    COMMIT;
END //

DELIMITER ;
