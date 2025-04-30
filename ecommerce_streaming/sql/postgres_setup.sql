CREATE TABLE public.raw_events (
    event_id VARCHAR(36),
    session_id VARCHAR(36),
    user_id VARCHAR(50),
    event_type VARCHAR(20),
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    brand VARCHAR(50),
    category VARCHAR(50),
    price DOUBLE PRECISION,
    quantity INTEGER,
    total_price DOUBLE PRECISION,
    event_date DATE,
    timestamp TIMESTAMP,
    PRIMARY KEY (event_id)
);


-- CREATE TABLE IF NOT EXISTS product_sales (
--     product_id VARCHAR(50) PRIMARY KEY,
--     product_name VARCHAR(255) NOT NULL,
--     brand VARCHAR(50) NOT NULL,
--     category VARCHAR(50) NOT NULL,
--     total_revenue DOUBLE PRECISION NOT NULL
-- );

-- CREATE TABLE IF NOT EXISTS top_products (
--     window_start TIMESTAMP NOT NULL,
--     window_end TIMESTAMP NOT NULL,
--     product_id VARCHAR(50) NOT NULL,
--     product_name VARCHAR(255) NOT NULL,
--     brand VARCHAR(50) NOT NULL,
--     purchase_count BIGINT NOT NULL,
--     PRIMARY KEY (window_start, product_id)
-- );

-- CREATE TABLE IF NOT EXISTS session_events (
--     session_id VARCHAR(36) PRIMARY KEY,
--     user_id VARCHAR(50) NOT NULL,
--     total_events BIGINT NOT NULL,
--     view_count BIGINT NOT NULL,
--     click_count BIGINT NOT NULL,
--     buy_count BIGINT NOT NULL
-- );
