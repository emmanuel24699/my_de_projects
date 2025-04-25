"""
Helper functions for the TMDB Movie Data Analysis pipeline.
Includes configuration, data fetching, cleaning, analysis, and visualization.
"""

import os
import json
import time
import logging
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import shutil
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    ArrayType, MapType, BooleanType, LongType, FloatType
)

# Setup logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration settings
load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259,
    99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513
]
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"
FIGURES_DIR = "reports/figures"

# TMDb schema for Spark DataFrame
tmdb_schema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("belongs_to_collection", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("backdrop_path", StringType(), True)
    ]), True),
    StructField("budget", LongType(), True),
    StructField("genres", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("homepage", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("origin_country", ArrayType(StringType()), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("poster_path", StringType(), True),
    StructField("production_companies", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("logo_path", StringType(), True),
        StructField("name", StringType(), True),
        StructField("origin_country", StringType(), True)
    ])), True),
    StructField("production_countries", ArrayType(StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", LongType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("spoken_languages", ArrayType(StructType([
        StructField("english_name", StringType(), True),
        StructField("iso_639_1", StringType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("title", StringType(), True),
    StructField("video", BooleanType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("credits", StructType([
        StructField("cast", ArrayType(StructType([
            StructField("adult", BooleanType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("original_name", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("profile_path", StringType(), True),
            StructField("cast_id", IntegerType(), True),
            StructField("character", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("order", IntegerType(), True)
        ])), True),
        StructField("crew", ArrayType(StructType([
            StructField("adult", BooleanType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("original_name", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("profile_path", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("department", StringType(), True),
            StructField("job", StringType(), True)
        ])), True)
    ]), True)
])

# Data Fetching Functions
def initialize_spark(app_name: str = "TMDB_Movie_Analysis") -> SparkSession:
    """
    Initialize a Spark session with custom configurations.
    
    Args:
        app_name: Name of the Spark application.
    
    Returns:
        SparkSession object.
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.python.worker.reuse", "false") \
            .config("spark.executor.pyspark.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.default.parallelism", "4") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
            .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
            .config("spark.network.timeout", "800s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .getOrCreate()
        logger.info("Spark session initialized with custom configurations")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark: {e}")
        raise

def load_cached_data(cache_dir: str = RAW_DATA_DIR) -> list:
    """
    Load cached movie data from the latest raw_movies_*.json file in cache_dir.
    
    Args:
        cache_dir: Directory containing cached JSON files.
    
    Returns:
        List of movie data dictionaries from the latest JSON file, or empty list if none found.
    """
    try:
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        json_files = sorted(cache_dir.glob("raw_movies_*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
        for file in json_files:
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"Loaded cached data from {file}")
                    return data if isinstance(data, list) else [data]
            except (PermissionError, json.JSONDecodeError) as e:
                logger.warning(f"Failed to load {file}: {e}")
        logger.info("No valid cached JSON files found")
        return []
    except Exception as e:
        logger.warning(f"Failed to load cached data: {e}")
        return []

def fetch_movie_data(movie_id: int, api_key: str = TMDB_API_KEY, base_url: str = BASE_URL, 
                     cache_dir: str = RAW_DATA_DIR, max_retries: int = 3) -> dict:
    """
    Fetch movie data for a given movie ID.
    
    Args:
        movie_id: Movie ID to fetch.
        api_key: TMDb API key.
        base_url: TMDb API base URL.
        max_retries: Maximum number of retry attempts.
    
    Returns:
        Movie data dictionary or None if fetch fails.
    """
    url = f"{base_url}/movie/{movie_id}?api_key={api_key}&append_to_response=credits"
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched data for movie ID {movie_id}")
            return data
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for movie ID {movie_id}: {e}")
            if attempt + 1 == max_retries:
                logger.error(f"Max retries reached for movie ID {movie_id}")
                return None
            time.sleep(2 ** attempt)
    return None

def create_movie_dataframe(movie_ids: list = MOVIE_IDS, api_key: str = TMDB_API_KEY, 
                          base_url: str = BASE_URL, cache_dir: str = RAW_DATA_DIR, 
                          spark: SparkSession = None) -> DataFrame:
    """
    Fetch movie data for a list of IDs and create a Spark DataFrame.
    
    Args:
        movie_ids: List of movie IDs.
        api_key: TMDb API key.
        base_url: TMDb API base URL.
        cache_dir: Directory for cached JSON files.
        spark: Active Spark session.
    
    Returns:
        Spark DataFrame with movie data.
    """
    cached_data = load_cached_data(cache_dir)
    cached_ids = {data.get('id') for data in cached_data if data.get('id')}
    movie_data = cached_data[:]
    failed_ids = []

    for movie_id in movie_ids:
        if movie_id not in cached_ids:
            data = fetch_movie_data(movie_id, api_key, base_url, cache_dir)
            if data and 'success' not in data:
                movie_data.append(data)
            else:
                logger.warning(f"Skipping movie ID {movie_id} due to fetch failure")
                failed_ids.append(movie_id)
        else:
            logger.info(f"Using cached data for movie ID {movie_id}")

    if not movie_data:
        logger.error("No movie data available")
        raise ValueError("No movie data available to create DataFrame")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_output = Path(cache_dir) / f"raw_movies_{timestamp}.json"
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    with open(raw_output, 'w') as f:
        json.dump(movie_data, f)
    logger.info(f"Saved combined movie data to {raw_output}")

    timestamp_file = Path(cache_dir) / "latest_timestamp.txt"
    with open(timestamp_file, 'w') as f:
        f.write(timestamp)
    logger.info(f"Latest timestamp recorded: {timestamp}")

    try:
        df = spark.createDataFrame(movie_data, schema=tmdb_schema)
        logger.info(f"Created Spark DataFrame with {df.count()} rows")
        if failed_ids:
            logger.info(f"Failed IDs: {failed_ids}")
        return df
    except Exception as e:
        logger.error(f"Failed to create Spark DataFrame: {e}")
        raise

# Data Cleaning Functions
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
        df = drop_irrelevant_columns(df)
        df = process_json_fields(df)
        
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
        
        save_parquet(df, output_dir)
        
        logger.info(f"Cleaned DataFrame has {df.count()} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to clean data: {e}")
        raise

# Analysis Functions
def rank_movies_udf(column: str, ascending: bool = False, top_n: int = 5) -> DataFrame:
    """
    UDF to rank movies based on a specified column.
    
    Args:
        column: Column to rank by (e.g., 'revenue_musd', 'vote_average').
        ascending: If True, rank in ascending order (e.g., for lowest values).
        top_n: Number of top/bottom movies to return.
    
    Returns:
        DataFrame with ranked movies.
    """
    def rank_df(df: DataFrame) -> DataFrame:
        try:
            ranked_df = (df.filter(F.col(column).isNotNull())
                        .orderBy(F.col(column).desc() if not ascending else F.col(column), 
                                 F.col("title"))
                        .limit(top_n)
                        .select("id", "title", column))
            return ranked_df
        except Exception as e:
            logger.error(f"Failed to rank movies by {column}: {e}")
            raise
    return rank_df

def compute_kpis(df: DataFrame) -> dict:
    """
    Compute KPIs for best/worst performing movies and print results.
    
    Args:
        df: Spark DataFrame with movie data.
    
    Returns:
        Dictionary of DataFrames containing KPI results.
    """
    try:
        kpi_results = {}
        print("Top 5 Movies by Revenue (MUSD):")
        kpi_results["highest_revenue"] = rank_movies_udf("revenue_musd")(df)
        kpi_results["highest_revenue"].show(truncate=False)

        print("Top 5 Movies by Budget (MUSD):")
        kpi_results["highest_budget"] = rank_movies_udf("budget_musd")(df)
        kpi_results["highest_budget"].show(truncate=False)

        print("Top 5 Movies by Profit (MUSD):")
        kpi_results["highest_profit"] = rank_movies_udf("profit_musd")(df)
        kpi_results["highest_profit"].show(truncate=False)

        print("Bottom 5 Movies by Profit (MUSD):")
        kpi_results["lowest_profit"] = rank_movies_udf("profit_musd", ascending=True)(df)
        kpi_results["lowest_profit"].show(truncate=False)

        print("Top 5 Movies by ROI (Budget >= 10M):")
        kpi_results["highest_roi"] = rank_movies_udf("roi")(df.filter(F.col("budget_musd") >= 10))
        kpi_results["highest_roi"].show(truncate=False)

        print("Bottom 5 Movies by ROI (Budget >= 10M):")
        kpi_results["lowest_roi"] = rank_movies_udf("roi", ascending=True)(df.filter(F.col("budget_musd") >= 10))
        kpi_results["lowest_roi"].show(truncate=False)

        print("Top 5 Most Voted Movies:")
        kpi_results["most_voted"] = rank_movies_udf("vote_count")(df)
        kpi_results["most_voted"].show(truncate=False)

        print("Top 5 Highest Rated Movies (>=10 votes):")
        kpi_results["highest_rated"] = rank_movies_udf("vote_average")(df.filter(F.col("vote_count") >= 10))
        kpi_results["highest_rated"].show(truncate=False)

        print("Bottom 5 Lowest Rated Movies (>=10 votes):")
        kpi_results["lowest_rated"] = rank_movies_udf("vote_average", ascending=True)(df.filter(F.col("vote_count") >= 10))
        kpi_results["lowest_rated"].show(truncate=False)

        print("Top 5 Most Popular Movies:")
        kpi_results["most_popular"] = rank_movies_udf("popularity")(df)
        kpi_results["most_popular"].show(truncate=False)

        return kpi_results
    except Exception as e:
        logger.error(f"Failed to compute KPIs: {e}")
        raise

def advanced_filtering(df: DataFrame) -> dict:
    """
    Perform advanced filtering and search queries.
    
    Args:
        df: Spark DataFrame with movie data.
    
    Returns:
        Dictionary of filtered DataFrames.
    """
    try:
        filter_results = {}
        sci_fi_action_willis = (df.filter(F.col("genres").like("%Science Fiction%") & 
                                         F.col("genres").like("%Action%") & 
                                         F.col("cast").like("%Bruce Willis%"))
                              .orderBy(F.col("vote_average").desc(), F.col("title"))
                              .select("title", "vote_average", "cast"))
        filter_results["sci_fi_action_willis"] = sci_fi_action_willis
        logger.info("Best-Rated Science Fiction Action Movies Starring Bruce Willis:")
        sci_fi_action_willis.show(truncate=False)

        thurman_tarantino = (df.filter(F.col("cast").like("%Uma Thurman%") & 
                                      F.col("director").like("%Quentin Tarantino%"))
                           .orderBy(F.col("runtime").asc(), F.col("title"))
                           .select("title", "runtime", "cast", "director"))
        filter_results["thurman_tarantino"] = thurman_tarantino
        logger.info("Movies Starring Uma Thurman, Directed by Quentin Tarantino (Sorted by Runtime):")
        thurman_tarantino.show(truncate=False)

        return filter_results
    except Exception as e:
        logger.error(f"Failed to perform advanced filtering: {e}")
        raise

def franchise_vs_standalone(df: DataFrame) -> DataFrame:
    """
    Compare franchise vs. standalone movie performance.
    
    Args:
        df: Spark DataFrame with movie data.
    
    Returns:
        DataFrame with comparison metrics.
    """
    try:
        comparison_df = (df.withColumn("movie_type", 
                                      F.when(F.col("belongs_to_collection").isNotNull(), "Franchise")
                                       .otherwise("Standalone"))
                        .groupBy("movie_type")
                        .agg(
                            F.avg("revenue_musd").alias("mean_revenue_musd"),
                            F.percentile_approx("roi", 0.5).alias("median_roi"),
                            F.avg("budget_musd").alias("mean_budget_musd"),
                            F.avg("popularity").alias("mean_popularity"),
                            F.avg("vote_average").alias("mean_rating")
                        ))
        logger.info("Franchise vs. Standalone Movie Performance:")
        comparison_df.show(truncate=False)
        return comparison_df
    except Exception as e:
        logger.error(f"Failed to compare franchise vs. standalone: {e}")
        raise

def analyze_franchises(df: DataFrame) -> DataFrame:
    """
    Identify the most successful movie franchises, and print results.
    
    Args:
        df: Spark DataFrame with movie data.
    
    Returns:
        DataFrame with franchise metrics.
    """
    try:
        franchise_df = (df.filter(F.col("belongs_to_collection").isNotNull())
                       .groupBy("belongs_to_collection")
                       .agg(
                           F.count("*").alias("movie_count"),
                           F.sum("budget_musd").alias("total_budget_musd"),
                           F.avg("budget_musd").alias("mean_budget_musd"),
                           F.sum("revenue_musd").alias("total_revenue_musd"),
                           F.avg("revenue_musd").alias("mean_revenue_musd"),
                           F.avg("vote_average").alias("mean_rating")
                       )
                       .orderBy(F.col("total_revenue_musd").desc()))
        logger.info("Most Successful Movie Franchises:")
        franchise_df.show(truncate=False)
        return franchise_df
    except Exception as e:
        logger.error(f"Failed to analyze franchises: {e}")
        raise

def analyze_directors(df: DataFrame) -> DataFrame:
    """
    Identify the most successful directors, and print results.
    
    Args:
        df: Spark DataFrame with movie data.
    
    Returns:
        DataFrame with director metrics.
    """
    try:
        director_df = (df.withColumn("director", F.explode(F.split("director", " \| ")))
                      .groupBy("director")
                      .agg(
                          F.count("*").alias("movie_count"),
                          F.sum("revenue_musd").alias("total_revenue_musd"),
                          F.avg("vote_average").alias("mean_rating")
                      )
                      .orderBy(F.col("total_revenue_musd").desc()))
        logger.info("Most Successful Directors:")
        director_df.show(truncate=False)
        return director_df
    except Exception as e:
        logger.error(f"Failed to analyze directors: {e}")
        raise

def analyze_data(df: DataFrame, output_dir: str = PROCESSED_DATA_DIR) -> tuple:
    """
    Orchestrate all analysis tasks and return updated DataFrame.
    
    Args:
        df: Spark DataFrame with movie data.
        output_dir: Directory for saving outputs.
    
    Returns:
        Tuple of updated DataFrame and analysis results dictionary.
    """
    try:
        results = {}
        df = df.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
        df = df.withColumn("roi", F.when(F.col("budget_musd") >= 10, 
                                F.col("revenue_musd") / F.col("budget_musd"))
                          .otherwise(None).cast(FloatType()))
        logger.info("Computing KPIs")
        results["kpis"] = compute_kpis(df)
        logger.info("Performing advanced filtering")
        results["filters"] = advanced_filtering(df)
        logger.info("Comparing franchise vs. standalone")
        results["franchise_vs_standalone"] = franchise_vs_standalone(df)
        logger.info("Analyzing franchises")
        results["franchises"] = analyze_franchises(df)
        logger.info("Analyzing directors")
        results["directors"] = analyze_directors(df)
        logger.info("All analysis tasks completed")
        return df, results
    except Exception as e:
        logger.error(f"Failed to analyze data: {e}")
        raise

# Visualization Functions
def plot_revenue_vs_budget(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Plot a scatter of revenue vs. budget.
    
    Args:
        df: Spark DataFrame with movie data.
        output_dir: Directory to save the plot.
    """
    try:
        df_pd = df.select("title", "budget_musd", "revenue_musd") \
                .filter(F.col("budget_musd").isNotNull() & F.col("revenue_musd").isNotNull()) \
                .toPandas()
        plt.figure(figsize=(10,6))
        sns.scatterplot(x="budget_musd", y="revenue_musd", data=df_pd, alpha=0.6)
        plt.title("Revenue vs Budget")
        plt.xlabel("Budget (MUSD)")
        plt.ylabel("Revenue (MUSD)")
        plt.grid(True)
        plt.show(block=False)
        plt.pause(5)
        plt.close()
        output_path = Path(output_dir) / "revenue_vs_budget.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved revenue vs. budget plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot revenue vs. budget: {e}")
        raise

def plot_roi_by_genre(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Plot a box plot of ROI distribution by genre.
    
    Args:
        df: Spark DataFrame with movie data.
        output_dir: Directory to save the plot.
    """
    try:
        df_exploded = df.select("roi", F.explode(F.split("genres", " \| ")).alias("genres")) \
                        .filter(F.col("roi").isNotNull())
        df_pd = df_exploded.toPandas()
        plt.figure(figsize=(10, 6))
        sns.boxplot(x="genres", y="roi", data=df_pd)
        plt.title("ROI Distribution by Genre")
        plt.xlabel("Genre")
        plt.ylabel("ROI (Revenue/Budget)")
        plt.xticks(rotation=45, ha="right")
        plt.grid(True, axis="y")
        plt.show(block=False)
        plt.pause(5)
        plt.close()
        output_path = Path(output_dir) / "roi_by_genre.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved ROI by genre plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot ROI by genre: {e}")
        raise

def plot_popularity_vs_rating(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Plot a scatter of popularity vs. rating.
    
    Args:
        df: Spark DataFrame with movie data.
        output_dir: Directory to save the plot.
    """
    try:
        df_pd = df.select("title", "popularity", "vote_average") \
                    .filter(F.col("popularity").isNotNull() & F.col("vote_average").isNotNull()) \
                    .toPandas()
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x="popularity", y="vote_average", data=df_pd, alpha=0.6)
        plt.title("Popularity vs. Rating")
        plt.xlabel("Popularity")
        plt.ylabel("Average Rating")
        plt.grid(True)
        plt.show(block=False)
        plt.pause(5)
        plt.close()
        output_path = Path(output_dir) / "popularity_vs_rating.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved popularity vs. rating plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot popularity vs. rating: {e}")
        raise

def plot_yearly_trends(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Plot yearly trends in average revenue and budget.
    
    Args:
        df: Spark DataFrame with movie data.
        output_dir: Directory to save the plot.
    """
    try:
        df_yearly = df.withColumn("year", F.year("release_date")) \
                        .filter(F.col("year").isNotNull()) \
                        .groupby("year") \
                        .agg(
                            F.avg("revenue_musd").alias("avg_revenue_musd"),
                            F.avg("budget_musd").alias("avg_budget_musd")) \
                        .orderBy("year") \
                        .toPandas()
        plt.figure(figsize=(10, 6))
        plt.plot(df_yearly["year"], df_yearly["avg_revenue_musd"], label="Average Revenue (MUSD)", marker="o")
        plt.plot(df_yearly["year"], df_yearly["avg_budget_musd"], label="Average Budget (MUSD)", marker="s")
        plt.title("Yearly Trends in Box Office Performance")
        plt.xlabel("Year")
        plt.ylabel("Amount (MUSD)")
        plt.legend()
        plt.grid(True)
        plt.show(block=False)
        plt.pause(5)
        plt.close()
        output_path = Path(output_dir) / "yearly_trends.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved yearly trends plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot yearly trends: {e}")
        raise

def plot_franchise_vs_standalone(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Plot a bar comparison of franchise vs. standalone movie success.
    
    Args:
        df: Spark DataFrame with movie data.
        output_dir: Directory to save the plot.
    """
    try:
        comparison_df = (df.withColumn("movie_type",
                                       F.when(F.col("belongs_to_collection").isNotNull(), "Franchise")
                                       .otherwise("Standalone"))
                            .groupBy("movie_type")
                            .agg(
                                F.avg("revenue_musd").alias("mean_revenue_musd"),
                                F.avg("roi").alias("mean_roi"),
                                F.avg("vote_average").alias("mean_rating")
                            )
                            .toPandas())
        metrics = ["mean_revenue_musd", "mean_roi", "mean_rating"]
        labels = ["Revenue (MUSD)", "ROI", "Rating"]
        franchise_data = comparison_df[comparison_df["movie_type"] == "Franchise"][metrics].iloc[0]
        standalone_data = comparison_df[comparison_df["movie_type"] == "Standalone"][metrics].iloc[0]
        x = range(len(metrics))
        width = 0.35
        plt.figure(figsize=(10, 6))
        plt.bar([i - width/2 for i in x], franchise_data, width, label="Franchise", color="skyblue")
        plt.bar([i + width/2 for i in x], standalone_data, width, label="Standalone", color="lightcoral")
        plt.title("Franchise vs. Standalone Movie Success")
        plt.xticks(x, labels)
        plt.ylabel("Value")
        plt.legend()
        plt.grid(True, axis="y")
        plt.show(block=False)
        plt.pause(5)
        plt.close()
        output_path = Path(output_dir) / "franchise_vs_standalone.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved franchise vs. standalone plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot franchise vs. standalone: {e}")
        raise

def visualize_data(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Orchestrate all visualization tasks.
    
    Args:
        df: Spark DataFrame with movie data (must include required columns).
        output_dir: Directory to save plots.
    """
    try:
        logger.info("Generating visualization plots")
        plot_revenue_vs_budget(df, output_dir)
        plot_roi_by_genre(df, output_dir)
        plot_popularity_vs_rating(df, output_dir)
        plot_franchise_vs_standalone(df, output_dir)
        plot_yearly_trends(df, output_dir)
        logger.info("All visualizations completed")
    except Exception as e:
        logger.error(f"Failed to generate visualizations: {e}")
        raise