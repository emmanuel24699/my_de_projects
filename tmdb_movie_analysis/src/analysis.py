import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from config import MOVIE_IDS,TMDB_API_KEY,BASE_URL,RAW_DATA_DIR, PROCESSED_DATA_DIR

def rank_movies(df, metric, ascending=False, min_votes=10, min_budget=10):
    """Rank movies based on a specified metric with optional filtering."""
    filtered_df = df.copy()
    if 'vote' in metric.lower():
        filtered_df = filtered_df[filtered_df['vote_count'] >= min_votes]
    if 'roi' in metric.lower() or 'profit' in metric.lower():
        filtered_df = filtered_df[filtered_df['budget_musd'] >= min_budget]
    filtered_df = filtered_df.sort_values(by=metric, ascending=ascending)
    return filtered_df

def calculate_metrics(df):
    """Calculate profit_musd and roi for the DataFrame."""
    df = df.copy()
    df['profit_musd'] = (df['revenue_musd'] - df['budget_musd']).round(2)
    df['roi'] = (df['revenue_musd'] / df['budget_musd']).round(2)
    return df

def compute_kpi_rankings(df):
    """Compute top/bottom 5 rankings for various KPIs."""
    kpis = {
        'highest_revenue': rank_movies(df, 'revenue_musd').head(5),
        'highest_budget': rank_movies(df, 'budget_musd').head(5),
        'highest_profit': rank_movies(df, 'profit_musd').head(5),
        'lowest_profit': rank_movies(df, 'profit_musd', ascending=True).head(5),
        'highest_roi': rank_movies(df, 'roi').head(5),
        'lowest_roi': rank_movies(df, 'roi', ascending=True).head(5),
        'most_voted': rank_movies(df, 'vote_count', min_votes=0).head(5),
        'highest_rated': rank_movies(df, 'vote_average').head(5),
        'lowest_rated': rank_movies(df, 'vote_average', ascending=True).head(5),
        'most_popular': rank_movies(df, 'popularity', min_votes=0).head(5),
    }
    return kpis

def filter_specific_movies(df):
    """Filter for specific movie categories (Bruce Willis Sci-Fi Action, Uma Thurman/Tarantino)."""
    # Best rated Sci-Fi Action movies with Bruce Willis
    sci_fi_action_bruce_willis = df[
        df['genres'].str.contains('Science Fiction', na=False) &
        df['genres'].str.contains('Action', na=False) &
        df['cast'].str.contains('Bruce Willis', na=False)
    ].sort_values(by='vote_average', ascending=False)

    # Movies with Uma Thurman directed by Quentin Tarantino
    uma_thurman_tarentino_directed = df[
        df['cast'].str.contains('Uma Thurman', na=False) &
        (df['director'] == 'Quentin Tarantino')
    ].sort_values(by='runtime')

    return {
        'sci_fi_action_bruce_willis': sci_fi_action_bruce_willis,
        'uma_thurman_tarentino_directed': uma_thurman_tarentino_directed
    }

def compute_franchise_standalone_stats(df):
    """Compute statistics for franchise vs. standalone movies."""
    franchise_df = df[df['belongs_to_collection'].notna()]
    standalone_df = df[df['belongs_to_collection'].isna()]

    franchise_stats = {
        'mean_revenue': franchise_df['revenue_musd'].mean(),
        'median_roi': franchise_df['roi'].median(),
        'mean_budget': franchise_df['budget_musd'].mean(),
        'mean_popularity': franchise_df['popularity'].mean(),
        'mean_rating': franchise_df['vote_average'].mean()
    }

    standalone_stats = {
        'mean_revenue': standalone_df['revenue_musd'].mean(),
        'median_roi': standalone_df['roi'].median(),
        'mean_budget': standalone_df['budget_musd'].mean(),
        'mean_popularity': standalone_df['popularity'].mean(),
        'mean_rating': standalone_df['vote_average'].mean()
    }

    return {
        'franchise_stats': franchise_stats,
        'standalone_stats': standalone_stats
    }

def compute_franchise_performance(df):
    """Aggregate performance metrics by franchise."""
    franchise_df = df[df['belongs_to_collection'].notna()]
    franchise_performance = franchise_df.groupby('belongs_to_collection', observed=False).agg({
        'title': 'count',
        'budget_musd': ['sum', 'mean'],
        'revenue_musd': ['sum', 'mean'],
        'vote_average': 'mean'
    }).sort_values(by=('revenue_musd', 'sum'), ascending=False)

    franchise_performance.columns = [
        'num_movies', 'total_budget_musd', 'mean_budget_musd',
        'total_revenue_musd', 'mean_revenue_musd', 'mean_rating'
    ]
    return franchise_performance

def compute_director_performance(df):
    """Aggregate performance metrics by director."""
    director_performance = df.groupby('director').agg({
        'title': 'count',
        'revenue_musd': ['sum'],
        'vote_average': 'mean'
    }).sort_values(by=('revenue_musd', 'sum'), ascending=False)

    director_performance.columns = [
        'num_movies', 'total_revenue_musd', 'mean_rating'
    ]
    return director_performance

def save_analysis_results(df, save_path=None):
    """Save the DataFrame with analysis results to a Parquet file named cleaned_analyzed_movies_{timestamp}.parquet.
    """
    if save_path:
        try:
            # Generate timestamp in format YYYYMMDD_HHMMSS
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Use PROCESSED_DATA_DIR from config
            output_dir = Path(PROCESSED_DATA_DIR)
            
            # Create filename with fixed pattern
            timestamped_filename = f"cleaned_analyzed_movies_{timestamp}.parquet"
            timestamped_path = output_dir / timestamped_filename
            
            # Create output directory if it doesn't exist
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save DataFrame to Parquet file
            df.to_parquet(timestamped_filename, engine='pyarrow', index=False)
            print(f"DataFrame saved successfully to {timestamped_path}")
            
            # Save timestamp to latest_timestamp.txt
            timestamp_file = output_dir / "latest_timestamp.txt"
            with open(timestamp_file, 'w') as f:
                f.write(timestamp)
            print(f"Timestamp saved successfully to {timestamp_file}")
            
        except Exception as e:
            print(f"Error saving DataFrame or timestamp: {e}")

def perform_analysis(df, save_path=None):
    """Perform comprehensive movie data analysis"""
    df = calculate_metrics(df)
    kpis = compute_kpi_rankings(df)
    specific_movies = filter_specific_movies(df)
    franchise_standalone = compute_franchise_standalone_stats(df)
    franchise_performance = compute_franchise_performance(df)
    director_performance = compute_director_performance(df)

    if save_path:
        save_analysis_results(df, save_path)

    return {
        'kpis': kpis,
        'sci_fi_action_bruce_willis': specific_movies['sci_fi_action_bruce_willis'],
        'uma_thurman_tarentino_directed': specific_movies['uma_thurman_tarentino_directed'],
        'franchise_stats': franchise_standalone['franchise_stats'],
        'standalone_stats': franchise_standalone['standalone_stats'],
        'franchise_performance': franchise_performance,
        'director_performance': director_performance
    }