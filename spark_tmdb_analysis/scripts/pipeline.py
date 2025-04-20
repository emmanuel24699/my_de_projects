"""
Main pipeline script to orchestrate the TMDB movie data analysis.
"""
from api_fetch import initialize_spark, create_movie_dataframe
from config import TMDB_API_KEY, BASE_URL, MOVIE_IDS, RAW_DATA_DIR
import logging
from pathlib import Path

# Setup logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.propagate = False

def run_pipeline():
    """
    Run the TMDB movie data analysis pipeline.
    """
    try:
        # Initialize Spark session
        spark = initialize_spark()
        
        # Fetch movie data and create DataFrame
        df = create_movie_dataframe(MOVIE_IDS, TMDB_API_KEY, BASE_URL, RAW_DATA_DIR, spark)
        
        # Cache raw DataFrame for performance
        df.cache()
        logger.info(f"Raw DataFrame cached with {df.count()} rows")
        
        # TODO: Add data cleaning, analysis, and visualization steps
        
        # Stop Spark session
        spark.stop()
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()