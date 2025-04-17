DELIMITER //

-- Trigger: Logs inventory changes after a new order detail is inserted
CREATE TRIGGER after_order_detail_insert
AFTER INSERT ON Order_Details
FOR EACH ROW
BEGIN
    -- Insert a record into Inventory_Logs to track the deduction in stock due to the new order detail entry.
    INSERT INTO Inventory_Logs (
        product_id,         
        change_date,        
        quantity_changed,   
        reason             
    )
    VALUES (
        NEW.product_id,
        NOW(),
        -NEW.quantity,                      
        CONCAT('Order #', NEW.order_id)    
    );
END //

DELIMITER ;
