import pandas as pd
import numpy as np
import os
from src.data_fetching import fetch_movie_data
from src.data_cleaning import clean_data
from src.analysis import perform_analysis
from src.visualization import create_visualization

def run_pipeline():
    print("Fetching movie data...")
    raw_df = fetch_movie_data()
    if raw_df.empty:
        print("No data fetched. Exiting pipeline.")
        return
    
    # Get the latest timestamp from the fetch step (written by fetch_movie_data)
    with open("data/latest_timestamp.txt", "r") as f:
        latest_timestamp = f.read().strip()

    # Clean the data
    print("Cleaning data...")
    cleaned_df = clean_data(raw_df, save_path=f"data/processed/cleaned_movies_{latest_timestamp}.parquet")

    # Perform analysis
    print("Performing analysis...")
    analysis_results = perform_analysis(cleaned_df)

    # Extract results from the dictionary
    kpis = analysis_results['kpis']
    sci_fi_action_bruce_willis = analysis_results['sci_fi_action_bruce_willis']
    uma_thurman_tarantino_directed = analysis_results['uma_thurman_tarentino_directed']
    franchise_stats = analysis_results['franchise_stats']
    standalone_stats = analysis_results['standalone_stats']
    franchise_performance = analysis_results['franchise_performance']
    director_performance = analysis_results['director_performance']

    # Update the DataFrame with analysis results (roi, profit_musd)
    analyzed_df = cleaned_df.copy()  # Avoid modifying cleaned_df directly
    analyzed_df['profit_musd'] = (analyzed_df['revenue_musd'] - analyzed_df['budget_musd']).round(2)
    analyzed_df['roi'] = (analyzed_df['revenue_musd'] - analyzed_df['budget_musd']).round(2)

    # Save the analyzed DataFrame to avoid overwriting cleaned data
    analyzed_path = f"data/processed/analyzed_movies_{latest_timestamp}.parquet"
    analyzed_df.to_parquet(analyzed_path, engine='pyarrow', index=False)
    print(f"Analyzed data saved to {analyzed_path}")

    # Step 4: Create visualizations
    print("Generating visualizations...")
    create_visualization(analyzed_df, output_dir="reports/figures")
    print("Pipeline completed successfully.")

if __name__ == "__main__":
    # Ensure data directories exist
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("reports/figures", exist_ok=True)
    
    # Run the pipeline
    run_pipeline()