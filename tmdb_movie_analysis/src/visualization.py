import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from config import FIGURES_DIR

def save_plot(filename, dpi=300):
    """Save plot to FIGURES_DIR with consistent formatting.
    """
    output_dir = Path(FIGURES_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.show(block=False)
    plt.pause(3)
    plt.close()
    print(f"{filename} saved to {output_dir}")

def plot_revenue_vs_budget(df):
    """Generate a scatter plot of revenue vs. budget."""
    print("Plotting Revenue vs Budget trends...")
    plt.figure(figsize=(12, 8))
    plt.scatter(df['budget_musd'], df['revenue_musd'], alpha=0.5, c='blue', edgecolors='w', s=100)
    plt.xlabel('Budget $M')
    plt.ylabel('Revenue $M')
    plt.title('Revenue vs Budget Trends')
    plt.grid(True, linestyle='--', alpha=0.7)
    save_plot('revenue_vs_budget.png')

def plot_roi_by_genre(df):
    """Generate a box plot of ROI distribution by top 10 genres."""
    genre_df = df.assign(genres=df['genres'].str.split(' | ')).explode('genres')
    top_genres = genre_df['genres'].value_counts().head(10).index
    roi_by_genre = [genre_df[genre_df['genres'] == genre]['roi'].dropna() for genre in top_genres]
    
    print(f"Plotting ROI Distribution by Genre for top genres...")
    plt.figure(figsize=(12, 8))
    plt.boxplot(roi_by_genre, labels=top_genres, vert=True, patch_artist=True, showfliers=False)
    plt.xlabel('Genre', fontsize=12)
    plt.ylabel('ROI', fontsize=12)
    plt.title('ROI Distribution by Genre (Top 10 Genres)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    save_plot('roi_by_genre.png')

def plot_popularity_vs_rating(df):
    """Generate a scatter plot of popularity vs. rating."""
    print("Plotting Popularity vs Rating...")
    plt.figure(figsize=(12, 8))
    plt.scatter(df['popularity'], df['vote_average'], alpha=0.5, c='green', edgecolors='w', s=100)
    plt.xlabel('Popularity')
    plt.ylabel('Rating')
    plt.title('Popularity vs Rating')
    plt.grid(True, linestyle='--', alpha=0.7)
    save_plot('popularity_vs_rating.png')

def plot_yearly_trends(df):
    """Generate a line plot of yearly box office performance (revenue and budget)."""
    df = df.copy()
    df['year'] = df['release_date'].dt.year
    yearly_performance = df.groupby('year').agg({
        'revenue_musd': 'sum',
        'budget_musd': 'sum'
    }).dropna()
    
    print("Plotting Yearly Trends in Box Office Performance...")
    plt.figure(figsize=(12, 8))
    plt.plot(yearly_performance.index, yearly_performance['revenue_musd'], label='Revenue', marker='o')
    plt.plot(yearly_performance.index, yearly_performance['budget_musd'], label='Budget', marker='o')
    plt.xlabel('Year')
    plt.ylabel('Amount $M')
    plt.title('Yearly Trends in Box Office Performance')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    save_plot('yearly_trends.png')

def plot_franchise_vs_standalone(df):
    """Generate a bar plot comparing franchise vs. standalone movie metrics."""
    franchise_df = df[df['belongs_to_collection'].notna()]
    standalone_df = df[df['belongs_to_collection'].isna()]
    metrics = ['Mean Revenue', 'Mean Budget', 'Mean ROI', 'Mean Popularity', 'Mean Rating']
    franchise_values = [
        franchise_df['revenue_musd'].mean(),
        franchise_df['budget_musd'].mean(),
        franchise_df['roi'].mean(),
        franchise_df['popularity'].mean(),
        franchise_df['vote_average'].mean()
    ]
    standalone_values = [
        standalone_df['revenue_musd'].mean(),
        standalone_df['budget_musd'].mean(),
        standalone_df['roi'].mean(),
        standalone_df['popularity'].mean(),
        standalone_df['vote_average'].mean()
    ]
    
    print("Plotting Franchise vs Standalone Success...")
    x = np.arange(len(metrics))
    width = 0.35
    plt.figure(figsize=(12, 8))
    plt.bar(x - width/2, franchise_values, width, label='Franchise', color='skyblue')
    plt.bar(x + width/2, standalone_values, width, label='Standalone', color='salmon')
    plt.ylabel('Average Values', fontsize=12)
    plt.xlabel('Metrics', fontsize=12)
    plt.title('Franchise vs Standalone Success', fontsize=14)
    plt.xticks(x, metrics, rotation=45, ha='right')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    save_plot('franchise_vs_standalone.png')

def create_visualization(df):
    """Generate and save visualizations for movie analysis results.
    
    Args:
        df (pd.DataFrame): DataFrame containing movie data with analysis results.
    """
    output_dir = Path(FIGURES_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    plot_revenue_vs_budget(df)
    plot_roi_by_genre(df)
    plot_popularity_vs_rating(df)
    plot_yearly_trends(df)
    plot_franchise_vs_standalone(df)
    
    print(f"Visualizations created successfully and saved to {output_dir}")