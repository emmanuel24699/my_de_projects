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
    try:
        logger.info(f"Inspecting value counts for {column}")
        counts = df.groupBy(column).count().orderBy(F.col("count").desc())
        counts_list = counts.collect()
        for row in counts_list:
            logger.info(f"{column}: {row[column]} -> {row['count']}")
    except Exception as e:
        logger.error(f"Failed to inspect {column}: {e}")

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
        # Drop irrelevant columns
        columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
        logger.info(f"Dropping irrelevant columns: {columns_to_drop}")
        df = df.drop(*columns_to_drop)

        # Process JSON-like fields
        logger.info("Processing JSON-like columns and credits")
        df = (df
            # belongs_to_collection -> collection_name
            .withColumn("belongs_to_collection", 
                F.when(F.col("belongs_to_collection").isNotNull(), 
                       F.col("belongs_to_collection.name")))
            # genres -> genres (|-separated)
            .withColumn("genres", 
                F.array_join(F.transform(F.col("genres"), 
                                        lambda x: x["name"]), " | "))
            # spoken_languages -> spoken_languages (|-separated, using english_name)
            .withColumn("spoken_languages", 
                F.array_join(F.transform(F.col("spoken_languages"), 
                                        lambda x: x["english_name"]), " | "))
            # production_countries -> production_countries (|-separated)
            .withColumn("production_countries", 
                F.array_join(F.transform(F.col("production_countries"), 
                                        lambda x: x["iso_3166_1"]), " | "))
            # production_companies -> production_companies (|-separated)
            .withColumn("production_companies", 
                F.array_join(F.transform(F.col("production_companies"), 
                                        lambda x: x["name"]), " | "))
            # cast -> all cast names (|-separated)
            .withColumn("cast", 
                F.array_join(
                    F.transform(F.col("credits.cast"), 
                                lambda x: x["name"]), " | "))
            # cast_size
            .withColumn("cast_size", 
                F.size(F.col("credits.cast")))
            # director -> director names (|-separated)
            .withColumn("director", 
                F.array_join(
                    F.transform(
                        F.filter(F.col("credits.crew"), 
                                 lambda x: x["job"] == "Director"), 
                        lambda x: x["name"]), " | "))
            # crew_size
            .withColumn("crew_size", 
                F.size(F.col("credits.crew")))
            # Drop credits and origin_country
            .drop("credits", "origin_country")
        )

        # Inspect extracted columns
        columns_to_inspect = [
            "belongs_to_collection", "genres", "spoken_languages",
            "production_countries", "production_companies", "cast", "director"
        ]
        for column in columns_to_inspect:
            inspect_column(df, column)
        
        # Convert datatypes
        logger.info("Converting data types")
        # budget and revenue to million USD (FloatType)
        df = df.withColumn("budget_musd", (F.col("budget") / 1_000_000).cast(FloatType()))
        df = df.withColumn("revenue_musd", (F.col("revenue") / 1_000_000).cast(FloatType()))
        df = df.drop("budget", "revenue")
        # popularity
        df = df.withColumn("popularity", F.col("popularity").cast(FloatType()))
        # id
        df = df.withColumn("id", F.col("id").cast(IntegerType()))
        # release_date to datetime
        df = df.withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))

        # Replace unrealistic values
        logger.info("Replacing unrealistic values")
        # Budget/Revenue = 0 -> null
        df = df.withColumn("budget_musd", F.when(F.col("budget_musd") <= 0, None).otherwise(F.col("budget_musd")))
        df = df.withColumn("revenue_musd", F.when(F.col("revenue_musd") <= 0, None).otherwise(F.col("revenue_musd")))
        # Runtime = 0 -> null
        df = df.withColumn("runtime", F.when(F.col("runtime") <= 0, None).otherwise(F.col("runtime")))
        # Vote_count = 0 -> set vote_average to null
        df = df.withColumn("vote_average", F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average")))
        # Overview/tagline placeholders -> null
        df = df.withColumn("overview", F.when(F.col("overview").isin("", "No Data"), None).otherwise(F.col("overview")))
        df = df.withColumn("tagline", F.when(F.col("tagline").isin("", "No Data"), None).otherwise(F.col("tagline")))
        
        # Remove duplicates and drop rows with unknown id or title
        logger.info("Removing duplicates and rows with missing id or title")
        df = df.dropDuplicates(["id", "title"])
        df = df.filter(F.col("id").isNotNull() & F.col("title").isNotNull())

        # Keep rows with at least 10 non-null columns
        logger.info("Filtering rows with at least 10 non-null columns")
        df = df.na.drop(thresh=10)

        # Filter Released movies and drop status
        logger.info("Filtering Released movies")
        df = df.filter(F.col("status") == "Released")
        df = df.drop("status")

        # Reorder columns
        final_columns = [
            'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
            'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
            'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
            'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
        ]
        logger.info(f"Reordering columns: {final_columns}")
        df = df.select([F.col(c) for c in final_columns])
        
        # Reset index

        # Save cleaned DataFrame as a single Parquet file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        temp_output_path = output_dir_path / f"temp_cleaned_movies_{timestamp}"
        final_output_path = output_dir_path / f"cleaned_movies_{timestamp}.parquet"

        logger.info(f"Saving single Parquet file to {final_output_path}")
        df.coalesce(1).write.mode("overwrite").parquet(str(temp_output_path))
        
        # Move the single part-*.parquet file to the final path
        for file in temp_output_path.glob("part-*.parquet"):
            shutil.move(file, final_output_path)
            logger.info(f"Moved Parquet file to {final_output_path}")
            break
        
        # Clean up the temporary directory
        shutil.rmtree(temp_output_path, ignore_errors=True)
        logger.info(f"Removed temporary directory: {temp_output_path}")

        # Save timestamp
        timestamp_file = output_dir_path / "latest_timestamp.txt"
        with open(timestamp_file, 'w') as f:
            f.write(timestamp)
        logger.info(f"Latest timestamp recorded: {timestamp}")

        # Log final row count
        logger.info(f"Cleaned DataFrame has {df.count()} rows")

        return df
    
    except Exception as e:
        logger.error(f"Failed to clean data: {e}")
        raise
        


