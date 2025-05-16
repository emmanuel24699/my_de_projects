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
