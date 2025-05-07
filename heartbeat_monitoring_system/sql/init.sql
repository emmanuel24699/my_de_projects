CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    heart_rate INTEGER NOT NULL,
    CONSTRAINT valid_heart_rate CHECK (heart_rate >= 0 AND heart_rate <= 300)
);

CREATE INDEX IF NOT EXISTS idx_timestamp ON heartbeats (timestamp);
CREATE INDEX IF NOT EXISTS idx_customer_id ON heartbeats (customer_id);