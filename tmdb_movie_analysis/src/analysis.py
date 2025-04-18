import pandas as pd
import numpy as np
import os

def rank_movies(df, metric, ascending=False, min_votes=10, min_budget=10):
    filtered_df = df.copy()
    if 'vote' in metric.lower():
        filtered_df = filtered_df[filtered_df['vote_count'] >= min_votes]
    if 'roi' in metric.lower() or 'profit' in metric.lower():
        filtered_df = filtered_df[filtered_df['budget_musd'] >= min_budget]
    filtered_df = filtered_df.sort_values(by=metric, ascending=ascending)
    return filtered_df

def perform_analysis(df):
    df['profit_musd'] = (df['revenue_musd'] - df['budget_musd']).round(2)
    df['roi'] = (df['revenue_musd'] / df['budget_musd']).round(2)

    # Identify best and worst movies 
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

    # Best rated Sci-fi action movies with Bruce Willis
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

    # Franchise vs Standalone Movie Performance
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
        'mean_rating': standalone_df['vote_average'].mean(),
    }

    # Most Successful Franchises
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

    # Most Successful Directors
    director_performance = df.groupby('director').agg({
        'title': 'count',
        'revenue_musd': ['sum'],
        'vote_average': 'mean'
    }).sort_values(by=('revenue_musd', 'sum'), ascending=False)

    director_performance.columns = [
        'num_movies', 'total_revenue_musd', 'mean_rating'
    ]

    return {
        'kpis': kpis,
        'sci_fi_action_bruce_willis': sci_fi_action_bruce_willis,
        'uma_thurman_tarentino_directed': uma_thurman_tarentino_directed,
        'franchise_stats': franchise_stats,
        'standalone_stats': standalone_stats,
        'franchise_performance': franchise_performance,
        'director_performance': director_performance
    }

if __name__ == "__main__":
    with open(f'data/latest_timestamp.txt', 'r') as f:
        latest_timestamp = f.read().strip()
    file_path = f"data/processed/cleaned_movies_{latest_timestamp}.parquet"
    df = pd.read_parquet(file_path, engine='pyarrow')
    analysis_results = perform_analysis(df)

    # Print summaries
    print("Top 5 Highest Revenue Movies:")
    print(analysis_results['kpis']['highest_revenue'][['title', 'revenue_musd']])

    print("Highest budget movies:")
    print(analysis_results['kpis']['highest_budget'][['title', 'budget_musd']])

    print("Highest profit movies:")
    print(analysis_results['kpis']['highest_profit'][['title', 'profit_musd']])

    print("Lowest profit movies:")
    print(analysis_results['kpis']['lowest_profit'][['title', 'profit_musd']])

    print("Highest ROI movies:")
    print(analysis_results['kpis']['highest_roi'][['title', 'roi']])

    print("Lowest ROI movies:")
    print(analysis_results['kpis']['lowest_roi'][['title', 'roi']])   

    print("Most voted movies:")
    print(analysis_results['kpis']['most_voted'][['title', 'vote_count']])

    print("Highest rated movies:")
    print(analysis_results['kpis']['highest_rated'][['title', 'vote_average']])

    print("Lowest rated movies:")
    print(analysis_results['kpis']['lowest_rated'][['title', 'vote_average']])

    print("Most popular movies:")
    print(analysis_results['kpis']['most_popular'][['title', 'popularity']])                                            

    print("\nBruce Willis Sci-Fi Action Movies:")
    print(analysis_results['sci_fi_action_bruce_willis'][['title', 'vote_average']])

    print("\nUma Thurman & Tarantino Movies:")
    print(analysis_results['uma_thurman_tarentino_directed'][['title', 'runtime']])

    print("\nFranchise Stats:")
    print(analysis_results['franchise_stats'])

    print("\nStandalone Stats:")
    print(analysis_results['standalone_stats'])

    print("\nTop 5 Franchises by Total Revenue:")
    print(analysis_results['franchise_performance'].head(5))

    print("\nTop 5 Directors by Total Revenue:")
    print(analysis_results['director_performance'].head(5))

    df.to_parquet(f"data/processed/cleaned_movies_{latest_timestamp}.parquet", engine='pyarrow', index=False)  # Save the updated DataFrame
    print("DataFrame saved successfully.")

