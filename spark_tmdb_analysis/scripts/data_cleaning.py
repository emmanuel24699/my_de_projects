"""
Functions for cleaning and preprocessing TMDb movie data in Spark.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType
from pathlib import Path
from datetime import datetime
import logging
import shutil
from config import PROCESSED_DATA_DIR

# Setup logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def inspect_column(df: DataFrame, column: str):
    """
    Display value counts for a specified column in a DataFrame-like format.
    
    Args:
        df: Spark DataFrame.
        column: Column name to inspect.
    """
    try:
        print(f"Inspecting value counts for {column}")
        counts = df.groupBy(column).count().orderBy(F.col("count").desc())
        counts.show(truncate=False)
    except Exception as e:
        print(f"Failed to inspect {column}: {e}")

def drop_irrelevant_columns(df: DataFrame) -> DataFrame:
    """
    Drop irrelevant columns from the DataFrame.
    
    Args:
        df: Input Spark DataFrame.
    Returns:
        DataFrame with irrelevant columns dropped.
    """
    try:
        columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
        logger.info(f"Dropping irrelevant columns: {columns_to_drop}")
        return df.drop(*columns_to_drop)
    except Exception as e:
        logger.error(f"Failed to drop irrelevant columns: {e}")
        raise

def process_json_fields(df: DataFrame) -> DataFrame:
    """
    Process JSON-like fields into |-separated strings and compute sizes.
    
    Args:
        df: Input Spark DataFrame.
    Returns:
        DataFrame with processed JSON fields.
    """
    try:
        logger.info("Processing JSON-like columns and credits")
        return (df
            .withColumn("belongs_to_collection", 
                F.when(F.col("belongs_to_collection").isNotNull(), 
                       F.col("belongs_to_collection.name")))
            .withColumn("genres", 
                F.array_join(F.transform(F.col("genres"), 
                                        lambda x: x["name"]), " | "))
            .withColumn("spoken_languages", 
                F.array_join(F.transform(F.col("spoken_languages"), 
                                        lambda x: x["english_name"]), " | "))
            .withColumn("production_countries", 
                F.array_join(F.transform(F.col("production_countries"), 
                                        lambda x: x["iso_3166_1"]), " | "))
            .withColumn("production_companies", 
                F.array_join(F.transform(F.col("production_companies"), 
                                        lambda x: x["name"]), " | "))
            .withColumn("cast", 
                F.array_join(
                    F.transform(F.col("credits.cast"), 
                                lambda x: x["name"]), " | "))
            .withColumn("cast_size", 
                F.size(F.col("credits.cast")))
            .withColumn("director", 
                F.array_join(
                    F.transform(
                        F.filter(F.col("credits.crew"), 
                                 lambda x: x["job"] == "Director"), 
                        lambda x: x["name"]), " | "))
            .withColumn("crew_size", 
                F.size(F.col("credits.crew")))
            .drop("credits", "origin_country")
        )
    except Exception as e:
        logger.error(f"Failed to process JSON fields: {e}")
        raise

def convert_data_types(df: DataFrame) -> DataFrame:
    """
    Convert columns to appropriate data types.
    
    Args:
        df: Input Spark DataFrame.
    Returns:
        DataFrame with corrected data types.
    """
    try:
        logger.info("Converting data types")
        return (df
            .withColumn("budget_musd", (F.col("budget") / 1_000_000).cast(FloatType()))
            .withColumn("revenue_musd", (F.col("revenue") / 1_000_000).cast(FloatType()))
            .drop("budget", "revenue")
            .withColumn("popularity", F.col("popularity").cast(FloatType()))
            .withColumn("id", F.col("id").cast(IntegerType()))
            .withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))
        )
    except Exception as e:
        logger.error(f"Failed to convert data types: {e}")
        raise

def replace_unrealistic_values(df: DataFrame) -> DataFrame:
    """
    Replace unrealistic values with null.
    
    Args:
        df: Input Spark DataFrame.
    Returns:
        DataFrame with unrealistic values set to null.
    """
    try:
        logger.info("Replacing unrealistic values")
        return (df
            .withColumn("budget_musd", F.when(F.col("budget_musd") <= 0, None).otherwise(F.col("budget_musd")))
            .withColumn("revenue_musd", F.when(F.col("revenue_musd") <= 0, None).otherwise(F.col("revenue_musd")))
            .withColumn("runtime", F.when(F.col("runtime") <= 0, None).otherwise(F.col("runtime")))
            .withColumn("vote_average", F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average")))
            .withColumn("overview", F.when(F.col("overview").isin("", "No Data"), None).otherwise(F.col("overview")))
            .withColumn("tagline", F.when(F.col("tagline").isin("", "No Data"), None).otherwise(F.col("tagline")))
        )
    except Exception as e:
        logger.error(f"Failed to replace unrealistic values: {e}")
        raise

def remove_duplicates_and_invalid_rows(df: DataFrame) -> DataFrame:
    """
    Remove duplicates and rows with missing id or title.
    
    Args:
        df: Input Spark DataFrame.
    Returns:
        DataFrame with duplicates and invalid rows removed.
    """
    try:
        logger.info("Removing duplicates and rows with missing id or title")
        return (df
            .dropDuplicates(["id", "title"])
            .filter(F.col("id").isNotNull() & F.col("title").isNotNull())
        )
    except Exception as e:
        logger.error(f"Failed to remove duplicates and invalid rows: {e}")
        raise

def filter_rows(df: DataFrame) -> DataFrame:
    """
    Filter rows with at least 10 non-null columns and Released status.
    
    Args:
        df: Input Spark DataFrame.
    Returns:
        Filtered DataFrame.
    """
    try:
        logger.info("Filtering rows with at least 10 non-null columns and Released status")
        return (df
            .na.drop(thresh=10)
            .filter(F.col("status") == "Released")
            .drop("status")
        )
    except Exception as e:
        logger.error(f"Failed to filter rows: {e}")
        raise

def reorder_columns(df: DataFrame) -> DataFrame:
    """
    Reorder columns to a specified order.
    
    Args:
        df: Input Spark DataFrame.
    Returns:
        DataFrame with reordered columns.
    """
    try:
        final_columns = [
            'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
            'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
            'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
            'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
        ]
        logger.info(f"Reordering columns: {final_columns}")
        return df.select([F.col(c) for c in final_columns])
    except Exception as e:
        logger.error(f"Failed to reorder columns: {e}")
        raise

def save_parquet(df: DataFrame, output_dir: str) -> None:
    """
    Save DataFrame as a single timestamped Parquet file.
    
    Args:
        df: Spark DataFrame to save.
        output_dir: Directory to save the Parquet file.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        temp_output_path = output_dir_path / f"temp_cleaned_movies_{timestamp}"
        final_output_path = output_dir_path / f"cleaned_movies_{timestamp}.parquet"

        logger.info(f"Saving single Parquet file to {final_output_path}")
        df.coalesce(1).write.mode("overwrite").parquet(str(temp_output_path))
        
        parquet_files = list(temp_output_path.glob("part-*.parquet"))
        if not parquet_files:
            logger.error("No Parquet file found in temporary directory")
            raise RuntimeError("Failed to create Parquet file")
        shutil.move(parquet_files[0], final_output_path)
        logger.info(f"Moved Parquet file to {final_output_path}")
        
        shutil.rmtree(temp_output_path, ignore_errors=True)
        logger.info(f"Removed temporary directory: {temp_output_path}")

        timestamp_file = output_dir_path / "latest_timestamp.txt"
        with open(timestamp_file, 'w') as f:
            f.write(timestamp)
        logger.info(f"Latest timestamp recorded: {timestamp}")
    except Exception as e:
        logger.error(f"Failed to save Parquet file: {e}")
        raise

def clean_data(df: DataFrame, output_dir: str = PROCESSED_DATA_DIR) -> DataFrame:
    """
    Clean and preprocess the TMDb movie DataFrame.
    
    Args:
        df: Input Spark DataFrame with raw movie data.
        output_dir: Directory to save the cleaned Parquet file.
    Returns:
        Cleaned Spark DataFrame.
    """
    try:
        # Apply cleaning steps
        df = drop_irrelevant_columns(df)
        df = process_json_fields(df)
        
        # Inspect extracted columns
        columns_to_inspect = [
            "belongs_to_collection", "genres", "spoken_languages",
            "production_countries", "production_companies", "director"
        ]
        for column in columns_to_inspect:
            inspect_column(df, column)
        
        df = convert_data_types(df)
        df = replace_unrealistic_values(df)
        df = remove_duplicates_and_invalid_rows(df)
        df = filter_rows(df)
        df = reorder_columns(df)
        
        # Save cleaned DataFrame
        save_parquet(df, output_dir)
        
        # Log final row count
        logger.info(f"Cleaned DataFrame has {df.count()} rows")
        
        return df
    
    except Exception as e:
        logger.error(f"Failed to clean data: {e}")
        raise