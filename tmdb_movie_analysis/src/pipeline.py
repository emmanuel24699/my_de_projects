from data_fetching import create_movie_dataframe
from config import TMDB_API_KEY, BASE_URL, MOVIE_IDS, RAW_DATA_DIR, PROCESSED_DATA_DIR
import logging
from pathlib import Path
from datetime import datetime

# Setup logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.propagate = False

def run_pipeline():
    """
    Main function to run the ETL pipeline.
    """
    try:
        # Fetch and process data
        logger.info("Starting ETL pipeline")
        logger.info("Starting Data Fetching process...")
        df_raw = create_movie_dataframe(MOVIE_IDS, TMDB_API_KEY, BASE_URL, RAW_DATA_DIR)
        logger.info("Data fetched")

        #

    except Exception as e:
        logger.error(f"Error during pipeline execution: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()
