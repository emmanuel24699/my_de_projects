# Test Cases: Real-Time Data Ingestion Pipeline

## Overview

This document provides manual test cases to validate the e-commerce data pipeline, comparing expected vs. actual outcomes for CSV generation, Spark processing, transformations, PostgreSQL writes, and performance.

## Test Cases

### Test Case 1: CSV File Generation

- **Description**: Verify `data_generator.py` creates valid CSVs.
- **Steps**:
  1. Run `python data_generator.py`.
  2. Check `data/input/` for CSVs (e.g., `events_2025-04-30T172441.csv`).
  3. Inspect CSV for columns: `event_id`, `session_id`, `user_id`, `event_type`, `product_id`, `product_name`, `brand`, `category`, `price`, `quantity`, `timestamp`.
- **Expected Outcome**:
  - CSVs generated every 10 seconds with 15 events.
  - Columns present, valid data (e.g., `event_type` in ["view", "click", "buy"], `price` > 0).
- **Actual Outcome**:

```markdown
| event_id                             | session_id                           | user_id     | event_type | product_id  | product_name                    | brand  | category | price  | quantity | timestamp                   |
| ------------------------------------ | ------------------------------------ | ----------- | ---------- | ----------- | ------------------------------- | ------ | -------- | ------ | -------- | --------------------------- |
| 50abba54-dd3f-485d-936d-4465f51cf6b4 | eac36629-6ef9-45c9-9ca5-b35aed48e2df | user_000709 | view       | prod_000021 | Star Wars Millennium Falcon Set | LEGO   | Toys     | 159.99 | 1        | 2025-04-29T21:16:28.151430Z |
| 508e2de0-fe66-4213-a1e1-0b368e20b0f0 | b48a4d2a-4fa7-4edc-b38e-ae9d26192215 | user_000835 | view       | prod_000006 | Air Max Sneakers                | Nike   | Fashion  | 129.99 | 1        | 2025-04-29T18:33:26.278854Z |
| 663cda1b-a44a-45c5-8aca-6dd0a4316c91 | 7944ec6a-12f3-450a-9ebb-41d31cacc3da | user_000737 | view       | prod_000010 | 501 Slim Fit Jeans              | Levi's | Fashion  | 59.99  | 1        | 2025-04-30T02:14:14.859745Z |
```

### Test Case 2: Spark Detects New CSV Files

- **Description**: Ensure Spark reads new CSVs.
- **Steps**:
  1. Run `docker-compose up -d`.
  2. Start `data_generator.py`.
  3. Monitor `logs/spark_streaming.log` for "Processing batch X".
- **Expected Outcome**:
  - Logs confirm batch processing every ~10 seconds.
  - CSVs moved to `data/archive/`.
- **Actual Outcome**:
  - Test passed. Files actually moves to `data/archive` after processing

### Test Case 3: Data Transformations

- **Description**: Validate Spark transformations.
- **Steps**:
  1. Connect to PostgreSQL: `docker exec -it <postgres-container-name> psql -U admin -d ecommerce`.
  2. Query: `SELECT event_type, price, quantity, total_price, event_date, timestamp FROM raw_events LIMIT 5;`.
  3. Check:
     - `total_price = price * quantity`.
     - `event_date` matches `DATE(timestamp)`.
- **Expected Outcome**:
  - `total_price` correct (e.g., `price=100`, `quantity=2` → `total_price=200`).
  - `event_date` matches `timestamp` date.
- **Actual Outcome**:

```markdown
| event_type | price   | quantity | total   | date       | timestamp                  |
| ---------- | ------- | -------- | ------- | ---------- | -------------------------- |
| view       | 1199.99 | 1        | 1199.99 | 2025-04-30 | 2025-04-30 09:26:13.987726 |
| click      | 1199.99 | 1        | 1199.99 | 2025-04-30 | 2025-04-30 09:28:36.947592 |
| view       | 399.99  | 1        | 399.99  | 2025-04-30 | 2025-04-30 08:33:45.271120 |
| view       | 129.99  | 1        | 129.99  | 2025-04-29 | 2025-04-29 16:53:13.409279 |
```

### Test Case 4: Data Written to PostgreSQL

- **Description**: Confirm data is upserted without errors.
- **Steps**:
  1. Run pipeline for some minutes.
  2. Query: `SELECT COUNT(*) FROM raw_events;`.
  3. Check `spark_streaming.log` for upsert errors.
- **Expected Outcome**:
  - Row count increases (~15-45 rows/batch, ~450 rows after some minutes).
  - No errors in logs.
- **Actual Outcome**:
  - 500

### Test Case 5: Performance Metrics

- **Description**: Measure latency and throughput.
- **Steps**:
  1. Run pipeline for 10 minutes.
  2. Calculate throughput: (total rows) / (seconds).
  3. Estimate latency from log timestamps (batch start to upsert).
- **Expected Outcome**:
  - Throughput: ~1.5 rows/second (15 events every 10 seconds).
  - Latency: <1 second/batch.
- **Actual Outcome**:
  - Throughput: ~1.4 rows/second
  - Latency: Batch start (2025-05-01 06:44:06) - Upsert time (2025-05-01 06:44:07) = ~1 second/batch

##
