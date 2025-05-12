 # Real-Time Customer Heart Beat Monitoring System

## Project Overview

This project implements a real-time data pipeline to simulate, process, and visualize heart rate data for multiple customers. It uses a synthetic data generator to mimic heart rate monitors, streams data via Apache Kafka, stores it in a PostgreSQL database, and visualizes it using a Grafana dashboard. The system is containerized with Docker Compose, making it easy to set up and test locally.

### Project Objectives

- Simulate real-time heart rate data for multiple customers.
- Stream data using Kafka producers and consumers.
- Process and validate data before storing it in a PostgreSQL database.
- Design a PostgreSQL schema for time-series data.
- Build and test the data pipeline with Docker Compose.
- Visualize data with a Grafana dashboard.

## System Architecture

The system comprises the following components:

1. **Synthetic Data Generator**: A Python script (`producer.py`) generates random heart rate data (`customer_id`, `timestamp`, `heart_rate`) for multiple customers.
2. **Kafka Producer**: Sends generated data to a Kafka topic (`heartbeats`).
3. **Kafka Consumer**: Reads messages from the `heartbeats` topic, validates data (e.g., heart rate between 30–200 bpm), and inserts valid records into PostgreSQL.
4. **PostgreSQL Database**: Stores heart rate data in a `heartbeats` table, indexed for efficient time-series queries.
5. **Grafana Dashboard**: Visualizes real-time heart rate data with gauge and bar gauge panels, connected to PostgreSQL.
6. **Docker Compose**: Orchestrates Zookeeper, Kafka, PostgreSQL, producer, consumer, and Grafana services.

### Data Flow Diagram

![Heart Rate Monitoring Diagram](/docs/system_architecture.png)

## Prerequisites

- **Docker** and **Docker Compose** installed.
- Python 3.8+ (for development or testing outside Docker).

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/heartbeat-monitoring-system.git
cd heartbeat-monitoring-system
```

### 2. Configure Environment Variables

Create a `.env` file in the project root to store PostgreSQL credentials:

```bash
# sample
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=db
POSTGRES_HOST=localhost

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

_Note_: Ensure the `.env` file is not committed to Git (excluded in `.gitignore`).

### 3. Directory Structure

```
heartbeat-monitoring-system/
├── docker/
│   ├── Dockerfile.kafka
│   ├── Dockerfile.postgres
│   ├── Dockerfile.producer
│   └── Dockerfile.consumer
├── grafana/
│   ├── dashboards/
│   │   └── heartbeat_dashboard.json
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasource.yml
│   │   ├── dashboards/
│   │   │   └── dashboard.yml
├── scripts/
│   ├── producer.py
│   └── consumer.py
├── sql/
│   └── init.sql
├── docs/
│   └── data_flow_diagram.png
├── .env
├── .gitignore
├── docker-compose.yml
└── README.md
```

### 4. Build and Run the System

Start all services using Docker Compose:

```bash
docker-compose build
docker-compose up -d
```

This launches:

- Zookeeper
- Kafka
- PostgreSQL
- Producer and consumer services
- Grafana (`localhost:3000`)

### 5. Verify Services

- **Check container status**:
  ```bash
  docker-compose ps
  ```
- **View logs** (e.g., for Grafana):
  ```bash
  docker-compose logs grafana
  ```

### 6. Access the Grafana Dashboard

- Open `http://localhost:3000` in a browser.
- Log in with username `admin` and password `admin`.
- Navigate to **Dashboards** > **Browse** > **General** > **Heartbeat Monitoring Dashboard**.
- The dashboard displays:
  - **Gauge Panel**: Average heart rate over the last 5 seconds.
  - **Bar Gauge Panel**: Latest heart rate per customer.

### 7. Query the Database

Connect to PostgreSQL to verify data:

```bash
docker exec -it <postgres-container-name> psql -U admin -d heartbeat_db
```

Run sample queries:

```sql
SELECT * FROM heartbeats LIMIT 10;
SELECT DISTINCT ON (customer_id) customer_id, heart_rate AS current_heart_rate FROM heartbeats ORDER BY customer_id, timestamp DESC;
```

### 8. Stop the System

```bash
docker-compose down
```

## Testing

### Test Scripts

- **Producer**: Ensure `scripts/producer.py` generates realistic heart rate data (30–200 bpm) and sends it to the `heartbeats` topic.
- **Consumer**: Verify `scripts/consumer.py` filters invalid data (e.g., heart rate <30 or >200) and inserts valid records into PostgreSQL.

### Sample Test Commands

- Check Kafka topic:
  ```bash
  docker exec -it <kafka-container-name> kafka-console-consumer --bootstrap-server kafka:9092 --topic heartbeats --from-beginning
  ```
- Verify database records:
  ```bash
  docker exec -it <postgres-container-name> psql -U admin -d heartbeat_db -c "SELECT COUNT(*) FROM heartbeats;"
  ```

## Database Schema

The PostgreSQL schema is defined in `sql/init.sql`:

```sql
CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    heart_rate INTEGER NOT NULL
);

CREATE INDEX idx_timestamp ON heartbeats (timestamp);
```

- **Fields**:
  - `id`: Unique record ID.
  - `customer_id`: Identifier for each customer.
  - `timestamp`: Time of heart rate measurement.
  - `heart_rate`: Heart rate value (bpm).
- **Index**: `idx_timestamp` optimizes time-series queries.

## Sample Outputs

### Database Query Result

```sql
heartbeat_db=# SELECT * FROM heartbeats LIMIT 10;
```

![Sample Record](/docs/sample_output.png)

### Grafana Dashboard Screenshot

![Grafana Dashboad](/docs/dashboard_screenshot.png)

[⚠️ Suspicious Content] # Real-Time Customer Heart Beat Monitoring System

## Project Overview

This project implements a real-time data pipeline to simulate, process, and visualize heart rate data for multiple customers. It uses a synthetic data generator to mimic heart rate monitors, streams data via Apache Kafka, stores it in a PostgreSQL database, and visualizes it using a Grafana dashboard. The system is containerized with Docker Compose, making it easy to set up and test locally.

### Project Objectives

- Simulate real-time heart rate data for multiple customers.
- Stream data using Kafka producers and consumers.
- Process and validate data before storing it in a PostgreSQL database.
- Design a PostgreSQL schema for time-series data.
- Build and test the data pipeline with Docker Compose.
- Visualize data with a Grafana dashboard.

## System Architecture

The system comprises the following components:

1. **Synthetic Data Generator**: A Python script (`producer.py`) generates random heart rate data (`customer_id`, `timestamp`, `heart_rate`) for multiple customers.
2. **Kafka Producer**: Sends generated data to a Kafka topic (`heartbeats`).
3. **Kafka Consumer**: Reads messages from the `heartbeats` topic, validates data (e.g., heart rate between 30–200 bpm), and inserts valid records into PostgreSQL.
4. **PostgreSQL Database**: Stores heart rate data in a `heartbeats` table, indexed for efficient time-series queries.
5. **Grafana Dashboard**: Visualizes real-time heart rate data with gauge and bar gauge panels, connected to PostgreSQL.
6. **Docker Compose**: Orchestrates Zookeeper, Kafka, PostgreSQL, producer, consumer, and Grafana services.

### Data Flow Diagram

![Heart Rate Monitoring Diagram](/docs/system_architecture.png)

## Prerequisites

- **Docker** and **Docker Compose** installed.
- Python 3.8+ (for development or testing outside Docker).

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/heartbeat-monitoring-system.git
cd heartbeat-monitoring-system
```

### 2. Configure Environment Variables

Create a `.env` file in the project root to store PostgreSQL credentials:

```bash
# sample
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=db
POSTGRES_HOST=localhost

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

_Note_: Ensure the `.env` file is not committed to Git (excluded in `.gitignore`).

### 3. Directory Structure

```
heartbeat-monitoring-system/
├── docker/
│   ├── Dockerfile.kafka
│   ├── Dockerfile.postgres
│   ├── Dockerfile.producer
│   └── Dockerfile.consumer
├── grafana/
│   ├── dashboards/
│   │   └── heartbeat_dashboard.json
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasource.yml
│   │   ├── dashboards/
│   │   │   └── dashboard.yml
├── scripts/
│   ├── producer.py
│   └── consumer.py
├── sql/
│   └── init.sql
├── docs/
│   └── data_flow_diagram.png
├── .env
├── .gitignore
├── docker-compose.yml
└── README.md
```

### 4. Build and Run the System

Start all services using Docker Compose:

```bash
docker-compose build
docker-compose up -d
```

This launches:

- Zookeeper
- Kafka
- PostgreSQL
- Producer and consumer services
- Grafana (`localhost:3000`)

### 5. Verify Services

- **Check container status**:
  ```bash
  docker-compose ps
  ```
- **View logs** (e.g., for Grafana):
  ```bash
  docker-compose logs grafana
  ```

### 6. Access the Grafana Dashboard

- Open `http://localhost:3000` in a browser.
- Log in with username `admin` and password `admin`.
- Navigate to **Dashboards** > **Browse** > **General** > **Heartbeat Monitoring Dashboard**.
- The dashboard displays:
  - **Gauge Panel**: Average heart rate over the last 5 seconds.
  - **Bar Gauge Panel**: Latest heart rate per customer.

### 7. Query the Database

Connect to PostgreSQL to verify data:

```bash
docker exec -it <postgres-container-name> psql -U admin -d heartbeat_db
```

Run sample queries:

```sql
SELECT * FROM heartbeats LIMIT 10;
SELECT DISTINCT ON (customer_id) customer_id, heart_rate AS current_heart_rate FROM heartbeats ORDER BY customer_id, timestamp DESC;
```

### 8. Stop the System

```bash
docker-compose down
```

## Testing

### Test Scripts

- **Producer**: Ensure `scripts/producer.py` generates realistic heart rate data (30–200 bpm) and sends it to the `heartbeats` topic.
- **Consumer**: Verify `scripts/consumer.py` filters invalid data (e.g., heart rate <30 or >200) and inserts valid records into PostgreSQL.

### Sample Test Commands

- Check Kafka topic:
  ```bash
  docker exec -it <kafka-container-name> kafka-console-consumer --bootstrap-server kafka:9092 --topic heartbeats --from-beginning
  ```
- Verify database records:
  ```bash
  docker exec -it <postgres-container-name> psql -U admin -d heartbeat_db -c "SELECT COUNT(*) FROM heartbeats;"
  ```

## Database Schema

The PostgreSQL schema is defined in `sql/init.sql`:

```sql
CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    heart_rate INTEGER NOT NULL
);

CREATE INDEX idx_timestamp ON heartbeats (timestamp);
```

- **Fields**:
  - `id`: Unique record ID.
  - `customer_id`: Identifier for each customer.
  - `timestamp`: Time of heart rate measurement.
  - `heart_rate`: Heart rate value (bpm).
- **Index**: `idx_timestamp` optimizes time-series queries.

## Sample Outputs

### Database Query Result

```sql
heartbeat_db=# SELECT * FROM heartbeats LIMIT 10;
```

![Sample Record](/docs/sample_output.png)

### Grafana Dashboard Screenshot

![Grafana Dashboad](/docs/dashboard_screenshot.png)
