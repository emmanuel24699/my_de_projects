"""
Configuration settings for the TMDB Spark project.
Loads API key from .env file and defines constants for API and file paths.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variables and define necessary variables
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259,
    99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513
]
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"
FIGURES_DIR = "reports/figures"