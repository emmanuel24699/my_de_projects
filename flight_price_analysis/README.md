# Flight Price Analysis Pipeline Documentation

## 1. Overview

This document details an end-to-end data pipeline for processing and analyzing flight price data from Bangladesh, built using Apache Airflow. The pipeline ingests raw CSV data into a MySQL staging table, validates and transforms it, computes key performance indicators (KPIs), and stores results in a PostgreSQL database for analytical purposes.

## 2. Pipeline Architecture

The pipeline is orchestrated by Apache Airflow and consists of the following components:

- **Data Source**: A CSV file [`Flight_Price_Dataset_of_Bangladesh.csv`](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh?resource=download) containing flight price data, located in the `data/` folder.
- **Staging Database**: MySQL for initial data ingestion and validation.
- **Analytics Database**: PostgreSQL for storing transformed data and KPIs.
- **Orchestration**: Airflow manages the workflow, with tasks defined in a DAG (`flight_price_pipeline_by_Emmanuel_Gligbe`) located in the `dags/` folder.
- **Processing**: Python scripts using Pandas, SQLAlchemy, and database connectors (PyMySQL, Psycopg2), with dependencies listed in `requirements.txt`.
- **Infrastructure**: Dockerized services (Airflow, MySQL, PostgreSQL) defined in `docker-compose.yml`.

### Architecture Diagram

<img src="./images/System Architecture.png" alt="System Architecture" width="840" height="500"/><br>

CSV File → MySQL (Staging) → Data Validation & Transformation → PostgreSQL (Analytics) → KPI Computation

### Folder Structure

```
flight_price_analysis/
├── dags/
│   └── flight_price_pipeline.py
├── data/
│   └── Flight_Price_Dataset_of_Bangladesh.csv
├── logs/
│   └── flight_pipeline.log
├── plugins/
├── sql/
│   └── init-mysql.sql
├── .env
├── .gitignore
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## 3. Execution Flow

The pipeline is defined in the Airflow DAG `flight_price_pipeline_by_Emmanuel_Gligbe`, which runs daily. The flow consists of four sequential tasks:

1. **Ingest CSV to MySQL**: Loads raw CSV data into a MySQL staging table.
2. **Validate and Transform Data**: Cleans, validates, and transforms the data, saving it as a processed CSV.
3. **Load to PostgreSQL**: Transfers the processed data to a PostgreSQL analytics table.
4. **Compute KPIs**: Calculates and stores KPIs in PostgreSQL.

### DAG Structure

![DAG structure](./images/DAG%20structure.png)

```
ingest_csv_to_mysql → validate_transform_data → load_to_postgres → compute_kpis
```

## 4. Airflow DAG and Task Descriptions

### DAG: `flight_price_pipeline_by_Emmanuel_Gligbe`

- **Description**: Orchestrates the ETL pipeline for flight price data.
- **Schedule**: Runs daily (`@daily`).
- **Start Date**: May 17, 2025.
- **Retries**: 3 attempts with a 5-minute delay between retries.
- **Tags**: `flight_price_pipeline`.

### Tasks

1. **ingest_csv_to_mysql** (`PythonOperator`)

   - **Purpose**: Loads the CSV file into the `raw_flights` table in MySQL.
   - **Function**: `load_csv_to_mysql`
   - **Details**:
     - Reads `Flight_Price_Dataset_of_Bangladesh.csv` from `/opt/airflow/data`.
     - Renames columns to standardized snake_case (e.g., `Source Name` to `source_name`).
     - Truncates the staging table before loading to ensure fresh data.
     - Uses SQLAlchemy with PyMySQL to insert data in chunks (1000 rows).
     - Logs the number of rows loaded or errors to `/opt/airflow/logs/flight_pipeline.log`.

2. **validate_transform_data** (`PythonOperator`)

   - **Purpose**: Validates and transforms the staged data, saving it as a processed CSV.
   - **Function**: `validate_transform_data`
   - **Details**:
     - Reads data from MySQL `raw_flights` table.
     - Applies validations:
       - Deduplicates based on `airline`, `source`, `destination`, `departure_datetime`, `total_fare_bdt`.
       - Ensures non-null values for critical columns (`airline`, `source`, `destination`, `departure_datetime`, `total_fare_bdt`).
       - Filters valid categories (e.g., `stopovers` in `['Direct', '1 Stop', '2 Stops']`).
       - Validates numeric ranges (e.g., non-negative fares, `days_before_departure` between 1 and 90).
       - Ensures `departure_datetime` is before `arrival_datetime`.
     - Converts data types (e.g., timestamps for dates, floats for fares).
     - Saves the cleaned data to `/opt/airflow/data/processed_flight_data.csv`.
     - Logs row counts and errors.

3. **load_to_postgres** (`PythonOperator`)

   - **Purpose**: Loads the processed CSV into the PostgreSQL `flights` table.
   - **Function**: `load_to_postgres`
   - **Details**:
     - Reads the processed CSV.
     - Creates the `flflight_kpis` table in PostgreSQL if it doesn’t exist, with appropriate column types (e.g., `VARCHAR`, `TIMESTAMP`, `DECIMAL`).
     - Uses SQLAlchemy with Psycopg2 to append data in chunks.
     - Logs the number of rows loaded or errors.

4. **compute_kpis** (`PythonOperator`)
   - **Purpose**: Computes KPIs and stores them in the PostgreSQL `flight_kpis` table.
   - **Function**: `compute_kpis`
   - **Details**:
     - Queries the `flights` table to compute:
       - Average total fare by `airline`, `seasonality`, `travel_class`.
       - Total bookings by `airline`, `seasonality`, `travel_class`.
       - Average duration by `airline`, `seasonality`, `travel_class`.
     - Adds a `last_updated` timestamp.
     - Stores results in the `flight_kpis` table, replacing existing data.
     - Logs the number of KPI rows computed or errors.

## 5. KPI Definitions and Computation Logic

The pipeline computes the following KPIs, stored in the `flight_kpis` table:

- **Average Fare by Airline**:
  - **Definition**: Mean `total_fare_bdt` grouped by `airline`, `seasonality`, and `travel_class`.
  - **Purpose**: Identifies pricing trends across airlines and seasons.
  - **Computation**: SQL query with `AVG(total_fare_bdt)` and `GROUP BY airline, seasonality, travel_class`.
- **Seasonal Fare Variation**:
  - **Definition**: Compares average fares across seasons (e.g., `Regular`, `Winter Holidays`, `Eid`, `Hajj`).
  - **Purpose**: Highlights price fluctuations during peak seasons.
  - **Computation**: Included in the above grouping by `seasonality`.
- **Booking Count by Airline**:
  - **Definition**: Total number of bookings (`COUNT(*)`) grouped by `airline`, `seasonality`, and `travel_class`.
  - **Purpose**: Measures airline popularity and booking trends.
  - **Computation**: SQL query with `COUNT(*)` and `GROUP BY airline, seasonality, travel_class`.
- **Average Duration**:
  - **Definition**: Mean `duration_hours` grouped by `airline`, `seasonality`, and `travel_class`.
  - **Purpose**: Analyzes flight duration trends.
  - **Computation**: SQL query with `AVG(duration_hours)` and `GROUP BY airline, seasonality, travel_class`.

## 6. Infrastructure Setup

The pipeline runs in a Dockerized environment defined in `docker-compose.yml`:

- **Services**:
  - **Postgres**: PostgreSQL 16 for the analytics database (`<POSTGRES_DB>`).
  - **MySQL**: MySQL 8.0 for the staging database (`<MYSQL_DATABASE>`).
  - **Airflow Services**: `airflow-init`, `airflow-webserver`, `airflow-scheduler`, built from a custom `Dockerfile`.
- **Volumes**:
  - Persistent storage for database data (`postgres-data`, `mysql-data`).
  - Shared volumes for Airflow DAGs (`dags/`), logs (`logs/`), plugins (`plugins/`), data (`data/`), and SQL scripts (`sql/`).
- **Networks**: All services communicate over a `flight-network` bridge network.
- **Healthchecks**: Ensure database and Airflow service readiness.
- **Environment**: Variables (e.g., database credentials) are loaded from the `.env` file.

The `Dockerfile` extends the `apache/airflow:2.10.2` image, installing dependencies from `requirements.txt` (e.g., Pandas, SQLAlchemy, PyMySQL, Psycopg2).

## 7. Starting Up & Connecting to Databases and Querying KPIs

This section explains how to start up, connect to the MySQL and PostgreSQL databases and query the KPI results using environment variables defined in the `.env` file. Users must configure their `.env` file with appropriate credentials.

### Environment Variables

The `.env` file should include the following database-related variables (replace dummy values with your own secure credentials):

```
MYSQL_USER=<your_mysql_user>
MYSQL_PASSWORD=<your_mysql_password>
MYSQL_DATABASE=<your_mysql_database>
MYSQL_ROOT_PASSWORD=<your_mysql_root_password>
POSTGRES_USER=<your_postgres_user>
POSTGRES_PASSWORD=<your_postgres_password>
POSTGRES_DB=<your_postgres_db>
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://<your_postgres_user>:<your_postgres_password>@postgres:5432/<your_postgres_db>
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_UID=50000
AIRFLOW_ADMIN_USERNAME=<your_admin_username>
AIRFLOW_ADMIN_PASSWORD=<your_admin_password>
AIRFLOW_ADMIN_FIRSTNAME=<your_admin_firstname>
AIRFLOW_ADMIN_LASTNAME=<your_admin_lastname>
AIRFLOW_ADMIN_EMAIL=<your_admin_email>
```

**Instructions**: Edit the `.env` file in the project root (`flight_price_analysis/.env`) to replace placeholder values (e.g., `<your_mysql_user>`, `<your_mysql_password>`) with secure credentials. Ensure the `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` uses the same PostgreSQL credentials as `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`.

**Starting the services**: Run the following command to spin up the serices.

```bash
  docker-compose up -d
```

**Access Airflow UI**: After the airflow webserver fully starts, visit `localhost:8080` to access the Airflow UI and login with your credentials specified in the `.env` file.

** Run the DAG**: The DAG `flight_price_pipeline_by_Emmanuel_Gligbe` is setup to run `@daily` at midnight but could be triggered manually.

### Connecting to Databases

#### MySQL (Staging Database)

- **Host**: `mysql` (Docker service name)
- **Port**: 3307 (mapped to 3306 internally)
- **Database**: `<MYSQL_DATABASE>`
- **Credentials**: `<MYSQL_USER>` / `<MYSQL_PASSWORD>`

**Command-Line Connection** (from docker):

```bash
docker exec -it <mysql_container_name> mysql -u <your_mysql_user> -p<your_mysql_password> <your_mysql_database>

```

#### PostgreSQL (Analytics Database)

- **Host**: `postgres` (Docker service name)
- **Port**: 5433 (mapped to 5432 internally)
- **Database**: `<POSTGRES_DB>`
- **Credentials**: `<POSTGRES_USER>` / `<POSTGRES_PASSWORD>`

Replace `<your_postgres_user>`, `<your_postgres_password>`, and `<your_postgres_db>` with the values from your `.env` file.

**Command-Line Connection** (from docker):

```bash
docker exec -it <your_postgres_container_name> psql -U <your_postgres_user> -d <your_postgres_db>

```

Enter the password (`<your_postgres_password>`) when prompted.

### Querying KPIs

The KPIs are stored in the PostgreSQL `flight_kpis` table. Below are example queries to retrieve the computed KPIs and "Most Popular Routes" KPI.

#### Query 1: Retrieve All KPIs

This query fetches the average fare, booking count, and average duration by airline, seasonality, and travel class.

```sql
SELECT airline, seasonality, travel_class,
       avg_total_fare_bdt, total_bookings, avg_duration_hours,
       last_updated
FROM flight_kpis
ORDER BY airline, seasonality, travel_class;
```

#### Query 2: Analyze Seasonal Fare Variation

This query compares average fares across seasons for a specific airline.

```sql
SELECT seasonality, travel_class, avg_total_fare_bdt
FROM flight_kpis
WHERE airline = 'Biman Bangladesh Airlines'
ORDER BY seasonality, travel_class;
```

#### Query 3: Most Popular Routes

The "Most Popular Routes" KPI can be computed from the `flights` table by counting bookings per source-destination pair.

```sql
SELECT source, destination, source_name, destination_name,
       COUNT(*) AS booking_count
FROM flights
GROUP BY source, destination, source_name, destination_name
ORDER BY booking_count DESC
LIMIT 10;
```

#### Notes

- Ensure the Airflow pipeline has run successfully to populate the `flight_kpis` and `flights` tables.
- Queries can be executed from within the Docker containers (e.g., using `docker exec` to access the `postgres` or `mysql` containers) or from the host machine using the mapped ports.
- For programmatic access, use SQLAlchemy or other Python libraries (e.g., `psycopg2` for PostgreSQL, `pymysql` for MySQL) with the credentials from your `.env` file.
- Keep your `.env` file secure and do not share it publicly, as it contains sensitive credentials.

## 8. Challenges and Resolutions

1. **Challenge**: Ensuring data consistency between MySQL and PostgreSQL.

   - **Resolution**: Used SQLAlchemy for reliable database interactions and chunked data loading to handle large datasets. Added healthchecks in `docker-compose.yml` to ensure database readiness before Airflow tasks.

2. **Challenge**: Handling missing or invalid data in the CSV.

   - **Resolution**: Implemented comprehensive validation in the `validate_transform_data` task, including deduplication, null checks, and range/type validations. Invalid rows are filtered out, and errors are logged to `logs/flight_pipeline.log`.

3. **Challenge**: Managing Airflow dependencies and Docker setup.

   - **Resolution**: Used a custom `Dockerfile` to install Python dependencies and configured `docker-compose.yml` with proper volume mappings and user permissions.

4. **Challenge**: Logging and debugging pipeline failures.
   - **Resolution**: Configured Python logging to write to both files (`logs/flight_pipeline.log`) and console, with detailed error messages for each task.

## 9. Future Improvements

- **Enhance Validation**: Add checks for valid airport codes using a reference dataset.
- **Monitoring**: Integrate Airflow alerts (e.g., email/Slack notifications) for pipeline failures.
- **Query Optimization**: Index the `flights` table for faster KPI computations.

## 10. Conclusion

The Flight Price Analysis pipeline automates the ETL process for Bangladesh flight data, from CSV ingestion to KPI computation. It leverages Airflow’s orchestration, Docker for deployment, and Python for data processing. With robust logging, validation, and secure database connectivity instructions, the pipeline is reliable and extensible. The provided connection details and queries enable easy access to KPI results, supporting further analytics and insights.
