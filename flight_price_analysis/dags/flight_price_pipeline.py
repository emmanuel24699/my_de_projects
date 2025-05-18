from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import logging
import os

# Load environment variables
MYSQL_USER = os.environ.get("MYSQL_USER")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_DB = os.environ.get("POSTGRES_DB")

# Configure logging
log_dir = "/opt/airflow/logs"
os.makedirs(log_dir, exist_ok=True)
logger = logging.getLogger("flight_pipeline")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# File handler
log_file = os.path.join(log_dir, "flight_pipeline.log")
try:
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info(f"Logging to {log_file}")
except Exception as e:
    print(f"Failed to initialize logging: {str(e)}")

# Stream handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# Task 1: Load CSV to MySQL
def load_csv_to_mysql():
    try:
        csv_path = "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv"
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@mysql/{MYSQL_DATABASE}")
        with engine.connect() as conn:
            conn.execute("TRUNCATE TABLE raw_flights")
        df = pd.read_csv(csv_path)
        df = df.rename(columns={
            "Source Name": "source_name", "Destination Name": "destination_name",
            "Departure Date & Time": "departure_datetime", "Arrival Date & Time": "arrival_datetime",
            "Duration (hrs)": "duration_hours", "Class": "travel_class", "Booking Source": "booking_source",
            "Base Fare (BDT)": "base_fare_bdt", "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
            "Total Fare (BDT)": "total_fare_bdt", "Aircraft Type": "aircraft_type",
            "Days Before Departure": "days_before_departure"
        })
        df.to_sql("raw_flights", con=engine, if_exists="append", index=False, chunksize=1000)
        logger.info(f"Loaded {len(df)} rows to MySQL raw_flights table")
    except Exception as e:
        logger.error(f"Failed to load CSV to MySQL: {str(e)}")
        raise

# Task 2: Validate and Transform Data
def validate_transform_data():
    try:
        engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@mysql/{MYSQL_DATABASE}")
        df = pd.read_sql("SELECT * FROM raw_flights", con=engine)
        logger.info(f"Initial row count: {len(df)}")
        # Deduplicate based on key columns
        df = df.drop_duplicates(subset=["airline", "source", "destination", "departure_datetime", "total_fare_bdt"])
        logger.info(f"Row count after deduplication: {len(df)}")

        critical_columns = ["airline", "source", "destination", "departure_datetime", "total_fare_bdt"]
        df = df.dropna(subset=critical_columns)
        valid_stopovers = ['Direct', '1 Stop', '2 Stops']
        valid_classes = ['Economy', 'First Class', 'Business']
        valid_booking_sources = ['Online Website', 'Travel Agency', 'Direct Booking']
        valid_seasons = ['Regular', 'Winter Holidays', 'Eid', 'Hajj']

        df = df[
            (df["stopovers"].isin(valid_stopovers)) &
            (df["travel_class"].isin(valid_classes)) &
            (df["booking_source"].isin(valid_booking_sources)) &
            (df["seasonality"].isin(valid_seasons)) &
            (df["days_before_departure"].between(1, 90))
        ]

        df["departure_datetime"] = pd.to_datetime(df["departure_datetime"], errors="coerce")
        df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"], errors="coerce")
        df["days_before_departure"] = pd.to_numeric(df["days_before_departure"], errors="coerce").astype("Int64")
        for col in ["duration_hours", "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
        for col in ["airline", "source", "source_name", "destination", "destination_name", "stopovers", "aircraft_type", "travel_class", "booking_source", "seasonality"]:
            df[col] = df[col].astype(str).str.strip()

        df = df[
            (df["base_fare_bdt"] >= 0) &
            (df["tax_surcharge_bdt"] >= 0) &
            (df["total_fare_bdt"] >= 0) &
            (df["duration_hours"] >= 0) &
            (df["departure_datetime"] < df["arrival_datetime"])
        ]
        df.columns = df.columns.str.lower()
        output_path = "/opt/airflow/data/processed_flight_data.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Validated and transformed {len(df)} rows, saved to {output_path}")
    except Exception as e:
        logger.error(f"Failed to validate/transform data: {str(e)}")
        raise

# Task 3: Load to PostgreSQL
def load_to_postgres():
    try:
        processed_csv_path = "/opt/airflow/data/processed_flight_data.csv"
        if not os.path.exists(processed_csv_path):
            logger.error(f"Processed CSV not found: {processed_csv_path}")
            raise FileNotFoundError(f"Processed CSV not found: {processed_csv_path}")
        engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres/{POSTGRES_DB}")
        with engine.connect() as conn:
            table_exists = conn.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = 'flights'
                )
            """).fetchone()[0]
            if not table_exists:
                conn.execute("BEGIN")
                conn.execute("""
                    CREATE TABLE flights (
                        airline VARCHAR(50), source VARCHAR(3), source_name VARCHAR(100),
                        destination VARCHAR(3), destination_name VARCHAR(100), departure_datetime TIMESTAMP,
                        arrival_datetime TIMESTAMP, duration_hours DECIMAL(10,2), stopovers VARCHAR(10),
                        aircraft_type VARCHAR(50), travel_class VARCHAR(20), booking_source VARCHAR(20),
                        base_fare_bdt DECIMAL(15,2), tax_surcharge_bdt DECIMAL(15,2), total_fare_bdt DECIMAL(15,2),
                        seasonality VARCHAR(20), days_before_departure INTEGER
                    )
                """)
                conn.execute("COMMIT")
                logger.info("Created flights table")
            else:
                logger.info("Flights table already exists")
        df = pd.read_csv(processed_csv_path)
        df.to_sql("flights", con=engine, if_exists="append", index=False, chunksize=1000)
        logger.info(f"Loaded {len(df)} rows to PostgreSQL flights table")
    except Exception as e:
        logger.error(f"Failed to load data to PostgreSQL: {str(e)}")
        raise

# Task 4: Compute KPIs
def compute_kpis():
    try:
        engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres/{POSTGRES_DB}")
        query = """
        SELECT airline, seasonality, travel_class, AVG(total_fare_bdt) AS avg_total_fare_bdt,
               COUNT(*) AS total_bookings, AVG(duration_hours) AS avg_duration_hours
        FROM flights
        GROUP BY airline, seasonality, travel_class
        """
        kpi_df = pd.read_sql(query, con=engine)
        kpi_df["last_updated"] = pd.Timestamp.now()
        kpi_df.to_sql("flight_kpis", con=engine, if_exists="replace", index=False)
        logger.info(f"Computed and stored {len(kpi_df)} KPI rows")
    except Exception as e:
        logger.error(f"Failed to compute KPIs: {str(e)}")
        raise

# Define the DAG
with DAG(
    dag_id="flight_price_pipeline_by_Emmanuel_Gligbe",
    description="Flight price ETL pipeline",
    tags=["flight_price_pipeline"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2025, 5, 18),
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Define tasks
    # Task 1: Load CSV to MySQL
    ingest_task = PythonOperator(
        task_id="ingest_csv_to_mysql",
        python_callable=load_csv_to_mysql
    )
    # Task 2: Validate and Transform Data
    validate_data_task = PythonOperator(
        task_id="validate_transform_data",
        python_callable=validate_transform_data
    )
    # Task 3: Load to PostgreSQL
    postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )
    # Task 4: Compute KPIs
    kpi_task = PythonOperator(
        task_id="compute_kpis",
        python_callable=compute_kpis
    )

    # Set task dependencies
    ingest_task >> validate_data_task >> postgres_task >> kpi_task