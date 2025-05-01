# User Guide: Running the Real-Time Data Ingestion Pipeline

## Prerequisites

- Docker and Docker Compose installed.
- Python 3.8+ (optional, for running `data_generator.py` locally).
- At least 8GB RAM for Docker containers.

## Project Setup

1. **Clone the Repository**:

   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Configure Environment**:

   - Create or verify the `.env` file:
     ```
     # Sample
     POSTGRES_DB=ecommerce
     POSTGRES_USER=admin
     POSTGRES_PASSWORD=securepassword
     POSTGRES_HOST=postgres
     POSTGRES_PORT=5432
     ```
   - Ensure `sql/postgres_setup.sql` defines the `raw_events` table.

3. **Set Up Directories**:
   - Create: `data/input/`, `data/archive/`, `data/checkpoints/`, `data/catalog/`, `logs/`.
   - Verify `scripts/spark_streaming_to_postgres.py` and `config/requirements.txt` exist.

## Running the Pipeline

1. **Build and Start Docker Containers**:

   ```bash
   docker-compose up build -d
   ```

   - This launches `postgres` and `spark` services.
   - `postgres` initializes the `ecommerce` database and `raw_events` table.
   - `spark` runs the streaming job.

2. **Generate Data**:

   - Run locally:
     ```bash
     python data_generator.py
     ```
   - Or inside the Spark container:
     ```bash
     docker exec -it <spark-container-name> python /scripts/data_generator.py
     ```
   - CSVs are generated in `data/input/` every 10 seconds with 15 events each.

3. **Monitor the Pipeline**:
   - View Spark logs:
     ```bash
     tail -f logs/spark_streaming.log
     ```
   - Check PostgreSQL data:
     ```bash
     docker exec -it <postgres-container-name> psql -U admin -d ecommerce
     SELECT COUNT(*) FROM raw_events;
     ```

## Stopping the Pipeline

1. Stop the data generator (`Ctrl+C`).
2. Stop containers:
   ```bash
   docker-compose down
   ```

## Troubleshooting

- **No data in PostgreSQL**: Verify CSVs in `data/input/`, check `spark_streaming.log`.
- **Spark errors**: Ensure PostgreSQL JDBC driver (`postgresql-42.7.4.jar`) is in `/opt/bitnami/spark/jars/`.
- **Permissions**: Confirm `data/` and `logs/` are owned by user `1000:1000`.

## Notes

- Processes up to 3 CSVs every 10 seconds (~15-45 events per batch).
- Archives CSVs to `data/archive/`.
- Checkpoints in `data/checkpoints/` ensure fault tolerance.
