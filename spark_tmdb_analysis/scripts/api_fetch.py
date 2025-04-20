"""
Functions for fetching movie data from the TMDb API and creating a Spark DataFrame.
"""
import requests
import json
import os
from pathlib import Path
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
from datetime import datetime
import logging
import time
from config import TMDB_API_KEY, BASE_URL, RAW_DATA_DIR, MOVIE_IDS

# Setup logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        # Find all raw_movies_*.json files and sort by modification time (newest first)
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
            time.sleep(2 ** attempt)  # Exponential backoff
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
    # Load cached data
    cached_data = load_cached_data(cache_dir)
    cached_ids = {data.get('id') for data in cached_data if data.get('id')}
    movie_data = cached_data[:]
    failed_ids = []

    # Fetch only new or missing movie IDs
    for movie_id in movie_ids:
        if movie_id not in cached_ids:
            data = fetch_movie_data(movie_id, api_key, base_url, cache_dir)
            if data and 'success' not in data:  # Skip failed requests (e.g., 404 responses)
                movie_data.append(data)
            else:
                logger.warning(f"Skipping movie ID {movie_id} due to fetch failure")
                failed_ids.append(movie_id)
        else:
            logger.info(f"Using cached data for movie ID {movie_id}")

    # Check if any data is available
    if not movie_data:
        logger.error("No movie data available")
        raise ValueError("No movie data available to create DataFrame")

    # Save combined data as timestamped JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_output = Path(cache_dir) / f"raw_movies_{timestamp}.json"
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    with open(raw_output, 'w') as f:
        json.dump(movie_data, f)
    logger.info(f"Saved combined movie data to {raw_output}")

    # Save timestamp
    timestamp_file = Path(cache_dir) / "latest_timestamp.txt"
    with open(timestamp_file, 'w') as f:
        f.write(timestamp)
    logger.info(f"Latest timestamp recorded: {timestamp}")

    # Create Spark DataFrame
    try:
        df = spark.createDataFrame([Row(**data) for data in movie_data])
        logger.info(f"Created Spark DataFrame with {df.count()} rows")
        if failed_ids:
            logger.info(f"Failed IDs: {failed_ids}")
        return df
    except Exception as e:
        logger.error(f"Failed to create Spark DataFrame: {e}")
        raise

def initialize_spark(app_name: str = "TMDB_Movie_Analysis") -> SparkSession:
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
            .getOrCreate()
        logger.info("Spark session initialized with custom configurations")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark: {e}")
        raise