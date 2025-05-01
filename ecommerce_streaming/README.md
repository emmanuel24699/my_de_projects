# Real-Time Data Ingestion Pipeline with Spark Structured Streaming & PostgreSQL

## Overview

This project implements a real-time data pipeline for an e-commerce platform, simulating user activity (view, click, buy events) and processing it using Apache Spark Structured Streaming, with storage in PostgreSQL. The pipeline generates synthetic event data, streams it through Spark for transformation, and upserts it into a PostgreSQL database, enabling real-time analytics.

### Key Features

- **Data Generation**: Simulates e-commerce events for 1000 users across 25 products, producing CSV files with 15 events every 10 seconds.
- **Real-Time Processing**: Uses Spark Structured Streaming to read, transform, and upsert events with sub-second latency.
- **Persistent Storage**: Stores events in a PostgreSQL `raw_events` table for downstream analytics.
- **Containerized Deployment**: Orchestrates Spark and PostgreSQL using Docker Compose for easy setup and scalability.

### Objectives

- Simulate and ingest streaming data.
- Process data in real time with Spark Structured Streaming.
- Store and verify data in PostgreSQL.
- Understand real-time pipeline architecture.
- Measure system performance (throughput, latency).

### Tools & Technologies

- **Apache Spark Structured Streaming**: For real-time data processing.
- **PostgreSQL**: For data storage.
- **Python**: For data generation.
- **SQL**: For database setup.
- **Docker**: For containerized deployment.

## Project Structure

The project includes the following key files and directories:

- `data_generator.py`: Generates synthetic e-commerce events as CSVs.
- `spark_streaming_to_postgres.py`: Spark job to process and upsert events.
- `postgres_setup.sql`: SQL script to create the `ecommerce` database and `raw_events` table.
- `docker-compose.yml`: Orchestrates Spark and PostgreSQL containers.
- `system_architecture.png`: Diagram showing the flow of the pipeline.
- `data/input/`: Directory for generated CSVs.
- `data/archive/`: Directory for processed CSVs.
- `data/checkpoints/`: Directory for Spark checkpoints.
- `data/catalog/`: Directory for product catalog (`products.csv`).
- `logs/`: Directory for Spark logs (`spark_streaming.log`).
- **Documentation**:
  - `project_overview.md`: Describes system components and data flow.
  - `user_guide.md`: Step-by-step instructions to run the pipeline.
  - `test_cases.md`: Manual test plan with expected vs. actual outcomes.
  - `performance_metrics.md`: Reports throughput, latency, and resource usage.

## Setup

### Prerequisites

- Docker and Docker Compose.
- Python 3.8+ (optional, for local `data_generator.py` execution).
- At least 8GB RAM for Docker containers.

### Installation

1. **Clone the Repository**:

   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Configure Environment**:

   - Create a `.env` file:
     ```
     # sample
     POSTGRES_DB=ecommerce
     POSTGRES_USER=admin
     POSTGRES_PASSWORD=securepassword
     POSTGRES_HOST=postgres
     POSTGRES_PORT=5432
     ```
   - Ensure `sql/postgres_setup.sql` exists to create the `raw_events` table.

3. **Set Up Directories**:
   - Create: `data/input/`, `data/archive/`, `data/checkpoints/`, `data/catalog/`.
   - Verify `scripts/spark_streaming_to_postgres.py`, `config/requirements.txt`, and `log/spark_streaming.log` exists.

## Usage

1. **Build Start Docker Containers**:

   ```bash
   docker-compose up -d
   ```

   - This starts `postgres` (initializes database) and `spark` (runs streaming job).

2. **Generate Data**:

   - Run locally:
     ```bash
     python3 data_generator.py
     ```
   - Or inside the Spark container:
     ```bash
     docker exec -it <spark-container-name> python3 /scripts/data_generator.py
     ```
   - Generates CSVs with 15 events every 10 seconds in `data/input/`.

3. **Monitor the Pipeline**:

   - Check Spark logs:
     ```bash
     tail -f logs/spark_streaming.log
     ```
   - Query PostgreSQL:
     ```bash
     docker exec -it <postgres-container-name> psql -U admin -d ecommerce
     SELECT COUNT(*) FROM raw_events;
     ```

4. **Stop the Pipeline**:
   - Stop data generator: `Ctrl+C`.
   - Stop containers:
     ```bash
     docker-compose down
     ```

See `user_guide.md` for detailed instructions and troubleshooting.

## Testing

The pipeline has been validated through manual tests outlined in `test_cases.md`. Key tests include:

- **CSV Generation**: Verify CSVs contain 15 events every 10 seconds.
- **Spark Processing**: Confirm Spark processes batches and archives CSVs.
- **Transformations**: Check lowercase `event_type`, correct `total_price`, and `event_date`.
- **PostgreSQL Writes**: Ensure ~450 rows after 5 minutes with no errors.
- **Performance**: Measure throughput (~1.5 events/second) and latency (<1 second/batch).

To run tests:

1. Follow `test_cases.md` steps.
2. Update "Actual Outcome" sections with results.

## Performance

Performance metrics are detailed in `performance_metrics.md`. Summary:

- **Throughput**: ~1.4 events/second (15 events every 10 seconds).
- **Latency**: ~0.3-0.9 seconds per batch.
- **Resource Usage**: ~0.5-1 CPU core, 1-2GB RAM (Spark), 1GB RAM (PostgreSQL).
- **Recommendations**: Increase batch size or reduce interval for higher throughput; scale Spark for larger datasets.

## System Architecture

The architecture diagram is described in `system_architecture.png` picture.

The pipeline flow is:

- Data Generator → Input Directory → Spark Structured Streaming → PostgreSQL.
- Spark archives CSVs and saves checkpoints after processing.
- Docker orchestrates the Spark and PostgreSQL services.

## Troubleshooting

- **No data in PostgreSQL**: Check CSVs in `data/input/` and `spark_streaming.log`.
- **Spark errors**: Verify PostgreSQL JDBC driver (`postgresql-42.7.4.jar`) in `/opt/bitnami/spark/jars/`.
- **Permissions**: Ensure `data/` and `logs/` are owned by user.

See `user_guide.md` for detailed troubleshooting.

## Future Improvements

- **Real-Time Analytics and Visualizations**: Add queries for aggregates (e.g., sales by category) and visualize.
- **Data Quality**: Implement validation for duplicates or invalid events.

##
