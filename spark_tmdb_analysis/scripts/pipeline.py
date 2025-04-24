"""
Main pipeline script to orchestrate the TMDB movie data analysis.
"""
from api_fetch import initialize_spark, create_movie_dataframe
from data_cleaning import clean_data
from analysis import analyze_data
from visualization import visualize_data
from config import TMDB_API_KEY, BASE_URL, MOVIE_IDS, RAW_DATA_DIR, PROCESSED_DATA_DIR, FIGURES_DIR


def run_pipeline():
    """
    Orchestrates the TMDB movie data analysis pipeline.

    This function initializes a Spark session, fetches movie data, cleans and preprocesses it,
    performs analysis, and finally stops the Spark session. It logs the progress and any errors
    encountered during the execution.

    Parameters:
    None

    Returns:
    None
    """
    try:
        # Initialize Spark session
        spark = initialize_spark()
        
        # Fetch movie data and create DataFrame
        df_raw = create_movie_dataframe(MOVIE_IDS, TMDB_API_KEY, BASE_URL, RAW_DATA_DIR, spark)
        
        # Cache raw DataFrame for performance
        df_raw.cache()
        logger.info(f"Raw DataFrame cached with {df_raw.count()} rows")

        # Clean and preprocess the DataFrame
        cleaned_df = clean_data(df_raw, PROCESSED_DATA_DIR)
        cleaned_df.cache()
        logger.info(f"Cleaned DataFrame with {cleaned_df.count()} rows")

        # Perform analysis
        updated_df, analysis_results = analyze_data(cleaned_df, PROCESSED_DATA_DIR)
        logger.info(f"Analysis completed")
        updated_df.cache()
        
        # Generate visualizations
        visualize_data(updated_df, FIGURES_DIR)
        logger.info(f"Visualizations saved to {FIGURES_DIR}")
   
        # Stop Spark session
        spark.stop()
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
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
        df_raw = create_movie_dataframe(MOVIE_IDS, TMDB_API_KEY, BASE_URL, RAW_DATA_DIR, spark)
        
        # Cache raw DataFrame for performance
        df_raw.cache()
        logger.info(f"Raw DataFrame cached with {df_raw.count()} rows")

        # Clean and preprocess the DataFrame
        cleaned_df = clean_data(df_raw, PROCESSED_DATA_DIR)
        cleaned_df.cache()
        logger.info(f"Cleaned DataFrame with {cleaned_df.count()} rows")

        # Perform analysis
        updated_df, analysis_results = analyze_data(cleaned_df, PROCESSED_DATA_DIR)
        logger.info(f"Analysis completed")
        
        # Generate visualizations
        visualize_data(updated_df, FIGURES_DIR)
        logger.info(f"Visualizations saved to {FIGURES_DIR}")
        
        # Stop Spark session
        spark.stop()
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()