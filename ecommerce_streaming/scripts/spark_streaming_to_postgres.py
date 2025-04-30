import logging
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from retrying import retry
import uuid
import psycopg2
from psycopg2 import sql

# Ensure the log file exists
log_dir = "/logs"
log_file = os.path.join(log_dir, "spark_streaming.log")
os.makedirs(log_dir, exist_ok=True)
if not os.path.exists(log_file):
    with open(log_file, 'w') as f:
        pass 

# Configure logging
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .getOrCreate()

# Define schema for input CSVs
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("timestamp", TimestampType(), False)
])

# Read streaming CSVs
input_path = "/data/input/"
logger.info("Checking input path: %s", input_path)
files = glob.glob(input_path + "*.csv")
logger.info("Found files: %s", files)

# Validate CSVs
for file in files:
    try:
        sample_df = spark.read.option("header", "true").schema(schema).csv(file)
        row_count = sample_df.count()
        logger.info("File %s: %d rows", file, row_count)
        if row_count == 0:
            logger.warning("File %s is empty", file)
    except Exception as e:
        logger.error("Failed to read file %s: %s", file, str(e))

# Check checkpoint directory
checkpoint_dir = "/data/checkpoints/raw_events"
try:
    os.makedirs(checkpoint_dir, exist_ok=True)
    logger.info("Checkpoint directory %s is accessible", checkpoint_dir)
except Exception as e:
    logger.error("Checkpoint directory %s is not accessible: %s", checkpoint_dir, str(e))

streaming_df = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .option("enforceSchema", "true") \
    .option("maxFilesPerTrigger", 3) \
    .option("cleanSource", "archive") \
    .option("sourceArchiveDir", "/data/archive") \
    .csv(input_path)

# Log schema before validation
logger.info("Input schema before validation: %s", streaming_df.schema.json())

# Validate non-null fields
streaming_df = streaming_df.dropna(how="any", subset=schema.fieldNames())
for field in schema.fields:
    streaming_df = streaming_df.filter(col(field.name).isNotNull())

# Apply transformations
transformed_df = streaming_df.select(
    col("event_id").cast(StringType()).alias("event_id"),
    col("session_id").cast(StringType()).alias("session_id"),
    col("user_id").cast(StringType()).alias("user_id"),
    lower(col("event_type")).cast(StringType()).alias("event_type"),
    col("product_id").cast(StringType()).alias("product_id"),
    col("product_name").cast(StringType()).alias("product_name"),
    col("brand").cast(StringType()).alias("brand"),
    col("category").cast(StringType()).alias("category"),
    round(col("price").cast(DoubleType()), 2).alias("price"),
    col("quantity").cast(IntegerType()).alias("quantity"),
    round((col("price") * col("quantity")).cast(DoubleType()), 2).alias("total_price"),
    to_date(col("timestamp")).alias("event_date"),
    col("timestamp").cast(TimestampType()).alias("timestamp")
).filter(col("event_type").isin("view", "click", "buy"))

# Additional validation for event_id
transformed_df = transformed_df.filter(col("event_id").isNotNull() & (col("event_id") != ""))

# Log schema after transformation
logger.info("Transformed schema: %s", transformed_df.schema.json())

# Debug: Inspect first batch of transformed_df
try:
    sample_stream_df = transformed_df.writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("debug_stream") \
        .trigger(processingTime="30 seconds") \
        .start()
    spark.sql("SELECT * FROM debug_stream LIMIT 5").show()
    event_counts = spark.sql("""
        SELECT event_type, COUNT(*) AS event_count
        FROM debug_stream
        GROUP BY event_type
    """).collect()
    for row in event_counts:
        logger.info("Event type %s count in debug stream: %d", row["event_type"], row["event_count"])
    sample_stream_df.awaitTermination(120)  # Wait 120 seconds
    logger.info("Successfully sampled transformed_df")
except Exception as e:
    logger.error("Failed to sample transformed_df: %s", str(e))

# Define PostgreSQL connection properties
pg_properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://postgres:5432/ecommerce",
    "dbname": "ecommerce",
    "host": "postgres",
    "port": "5432"
}

if not pg_properties["user"] or not pg_properties["password"]:
    logger.error("POSTGRES_USER or POSTGRES_PASSWORD not set")
    raise ValueError("POSTGRES_USER or POSTGRES_PASSWORD not set")

# Test PostgreSQL connection
try:
    spark.createDataFrame([], StructType([])).write \
        .format("jdbc") \
        .option("url", pg_properties["url"]) \
        .option("dbtable", "test_connection") \
        .option("user", pg_properties["user"]) \
        .option("password", pg_properties["password"]) \
        .option("driver", pg_properties["driver"]) \
        .mode("overwrite") \
        .save()
    logger.info("PostgreSQL connection test successful")
except Exception as e:
    logger.error("PostgreSQL connection test failed: %s", str(e))
    raise

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def upsert_to_postgres(df, batch_id, table_name, key_columns):
    """Upsert DataFrame to PostgreSQL using a temporary table and psycopg2."""
    try:
        logger.info("Processing batch %d for table %s with %d rows", batch_id, table_name, df.count())
        if df.isEmpty():
            logger.info("No data to upsert for table %s, batch %d", table_name, batch_id)
            return

        # Create a unique temporary table name
        temp_table = f"temp_{table_name}_{batch_id}_{str(uuid.uuid4()).replace('-', '_')}"
        
        # Define schema for temporary table
        schema_sql = ", ".join([
            '"event_id" VARCHAR(36)',
            '"session_id" VARCHAR(36)',
            '"user_id" VARCHAR(50)',
            '"event_type" VARCHAR(20)',
            '"product_id" VARCHAR(50)',
            '"product_name" VARCHAR(255)',
            '"brand" VARCHAR(50)',
            '"category" VARCHAR(50)',
            '"price" DOUBLE PRECISION',
            '"quantity" INTEGER',
            '"total_price" DOUBLE PRECISION',
            '"event_date" DATE',
            '"timestamp" TIMESTAMP'
        ])
        
        # Create temporary table
        conn = None
        try:
            conn = psycopg2.connect(
                dbname=pg_properties["dbname"],
                user=pg_properties["user"],
                password=pg_properties["password"],
                host=pg_properties["host"],
                port=pg_properties["port"]
            )
            cursor = conn.cursor()
            create_table_sql = f"CREATE TEMPORARY TABLE {temp_table} ({schema_sql})"
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info("Created temporary table %s", temp_table)
        except Exception as e:
            logger.error("Failed to create temporary table %s: %s", temp_table, str(e))
            raise
        finally:
            if conn:
                cursor.close()
                conn.close()

        # Write DataFrame to temporary table
        try:
            df.write.format("jdbc") \
                .option("url", pg_properties["url"]) \
                .option("dbtable", f"public.{temp_table}") \
                .option("user", pg_properties["user"]) \
                .option("password", pg_properties["password"]) \
                .option("driver", pg_properties["driver"]) \
                .mode("append") \
                .save()
            logger.info("Wrote DataFrame to temporary table %s", temp_table)
        except Exception as e:
            logger.error("Failed to write DataFrame to temporary table %s: %s", temp_table, str(e))
            raise

        # Construct upsert SQL
        columns = df.columns
        key_conditions = ", ".join([f'"{col}"' for col in key_columns])
        update_columns = [col for col in columns if col not in key_columns]
        set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
        insert_columns = ", ".join([f'"{col}"' for col in columns])

        upsert_sql = f"""
        INSERT INTO public.{table_name} ({insert_columns})
        SELECT {insert_columns} FROM public.{temp_table}
        ON CONFLICT ({key_conditions})
        DO UPDATE SET {set_clause};
        """

        # Execute upsert using psycopg2
        conn = None
        try:
            conn = psycopg2.connect(
                dbname=pg_properties["dbname"],
                user=pg_properties["user"],
                password=pg_properties["password"],
                host=pg_properties["host"],
                port=pg_properties["port"]
            )
            cursor = conn.cursor()
            cursor.execute(upsert_sql)
            conn.commit()
            logger.info("Successfully executed upsert SQL for table %s, batch %d", table_name, batch_id)
        except Exception as e:
            logger.error("Failed to execute upsert SQL for table %s, batch %d: %s", table_name, batch_id, str(e))
            raise
        finally:
            if conn:
                cursor.close()
                conn.close()

        # Drop temporary table
        conn = None
        try:
            conn = psycopg2.connect(
                dbname=pg_properties["dbname"],
                user=pg_properties["user"],
                password=pg_properties["password"],
                host=pg_properties["host"],
                port=pg_properties["port"]
            )
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS public.{temp_table}")
            conn.commit()
            logger.info("Dropped temporary table %s", temp_table)
        except Exception as e:
            logger.error("Failed to drop temporary table %s: %s", temp_table, str(e))
        finally:
            if conn:
                cursor.close()
                conn.close()

        logger.info("Successfully upserted to PostgreSQL table %s, batch %d", table_name, batch_id)
    except Exception as e:
        logger.error("Failed to upsert to PostgreSQL table %s, batch %d: %s", table_name, batch_id, str(e))
        # Log error but don't raise to prevent stream termination

# Write stream to PostgreSQL
logger.info("Starting raw events query")
try:
    raw_events_query = transformed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, batch_id: upsert_to_postgres(df, batch_id, "raw_events", ["event_id"])) \
        .option("checkpointLocation", f"{checkpoint_dir}") \
        .trigger(processingTime="10 seconds") \
        .start()
    logger.info("Raw events query started with checkpoint %s", checkpoint_dir)
except Exception as e:
    logger.error("Failed to start raw events query: %s", str(e))
    raise

# Log start of streaming
logger.info("Started streaming query: raw_events")

# Await termination
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    logger.error("Streaming terminated with error: %s", str(e))
    raise
finally:
    spark.stop()
    logger.info("Spark session stopped")