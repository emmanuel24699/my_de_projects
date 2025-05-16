# Project Overview: Real-Time Data Ingestion with Spark Structured Streaming & PostgreSQL

## System Components

This project implements a real-time data pipeline simulating an e-commerce platform’s user activity. It uses Apache Spark Structured Streaming to process events and stores them in PostgreSQL. The key components are:

1. **Data Generator (`data_generator.py`)**:

   - Generates synthetic e-commerce events for 1000 users across 25 products in 5 categories.
   - Produces CSV files every 10 seconds in `data/input/`, each with 15 events containing `event_id`, `user_id`, `product_id`, `price`, `timestamp`, etc.
   - Saves a product catalog to `data/catalog/products.csv`.

2. **Spark Streaming Job (`spark_streaming_to_postgres.py`)**:

   - Monitors `data/input/` for new CSV files using Spark Structured Streaming.
   - Transforms data: computes `total_price`, extracts `event_date`, and filters valid event types (view, click, buy).
   - Upserts data into the `raw_events` table in PostgreSQL.
   - Archives processed CSVs to `data/archive/` and uses checkpoints in `data/checkpoints/` for fault tolerance.

3. **PostgreSQL Database (`postgres_setup.sql`)**:

   - Defines the `raw_events` table with fields like `event_id` (primary key), `user_id`, `event_type`, etc.

4. **Docker Orchestration (`docker-compose.yml`)**:
   - Manages `postgres` (PostgreSQL 16) and `spark` (Bitnami Spark 3.5.3) services.
   - Maps volumes for data, scripts, and logs, with Spark depending on a healthy PostgreSQL.

## Data Flow

1. The data generator writes timestamped CSV files to `data/input/` every 10 seconds.
2. Spark Structured Streaming reads new CSVs every 10 seconds, processes up to 3 files per batch, and applies transformations.
3. Transformed data is written to a temporary PostgreSQL table, then upserted into `raw_events` using `event_id`.
4. Processed CSVs are archived, and checkpoints ensure exactly-once processing.

## Purpose

The pipeline enables real-time tracking of e-commerce user behavior, supporting analytics like sales trends and user engagement.
