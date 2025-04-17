import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def create_visualization(df, output_dir="reports/figures"):
    """Generate and save visualizations for movie analysis results."""

    os.makedirs(output_dir, exist_ok=True)

    # Revenue vs Budget trends 
    print("Plotting Revenue vs Budget trends...")
    plt.figure(figsize=(12, 8))
    plt.scatter(df['budget_musd'], df['revenue_musd'], alpha=0.5, c='blue', edgecolors='w', s=100)
    plt.xlabel('Budget $M')
    plt.ylabel('Revenue $M')
    plt.title('Revenue vs Budget Trends')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.savefig(os.path.join(output_dir, 'revenue_vs_budget.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print('revenue_vs_budget.png saved to reports/figures')

    # ROI Distribution by Genre (Box Plot)
    genre_df = df.assign(genres=df['genres'].str.split(' | ')).explode('genres')
    top_genres = genre_df['genres'].value_counts().head(10).index
    roi_by_genre = [genre_df[genre_df['genres'] == genre]['roi'].dropna() for genre in top_genres]

    print(f"Plotting ROI Distribution by Genre for top genres...")
    plt.figure(figsize=(12, 8))
    plt.boxplot(roi_by_genre, tick_labels=top_genres, vert=True, patch_artist=True, showfliers=False)
    plt.xlabel('Genre', fontsize=12)
    plt.ylabel('ROI', fontsize=12)
    plt.title('ROI Distribution by Genre (Top 10 Genres)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.savefig(os.path.join(output_dir, 'roi_by_genre.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print('roi_by_genre.png saved to reports/figures')

    # Popularity vs Rating (Scatter Plot)
    print("Plotting Popularity vs Rating...")
    plt.figure(figsize=(12, 8))
    plt.scatter(df['popularity'], df['vote_average'], alpha=0.5, c='green', edgecolors='w', s=100)
    plt.xlabel('Popularity')
    plt.ylabel('Rating')
    plt.title('Popularity vs Rating')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.savefig(os.path.join(output_dir, 'popularity_vs_rating.png'), dpi=300, bbox_inches='tight')
    print('popularity_vs_rating.png saved to reports/figures')
    plt.close()

    # Yearly Trends in Box Office Performance (Line Plot)
    df['year'] = df['release_date'].dt.year
    yearly_performance = df.groupby('year').agg({
        'revenue_musd' : 'sum',
        'budget_musd' : 'sum'
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
    plt.savefig(os.path.join(output_dir, 'yearly_trends.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print('yearly_trends.png saved to reports/figures')
    
    franchise_df = df[df['belongs_to_collection'].notna()]
    standalone_df = df[df['belongs_to_collection'].isna()]
    metrics = ['mean_revenue', 'mean_budget', 'mean_roi', 'mean_popularity', 'mean_rating']
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
    x = np.arange(len(metrics))  # the label locations
    width = 0.35
    plt.figure(figsize=(12, 8))
    plt.bar(x - width/2, franchise_values, width, label='Franchise', color='skyblue')
    plt.bar(x + width/2, standalone_values, width, label='Standalone', color='salmon')
    plt.ylabel('Average Values', fontsize=12)
    plt.xlabel('Metrics', fontsize=12)
    plt.title('Franchise vs Standalone Success', fontsize=14)
    plt.xticks(x, ['Mean Revenue', 'Mean Budget', 'Mean ROI', 'Mean Popularity', 'Mean Rating'], rotation=45, ha='right')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.savefig(os.path.join(output_dir, 'franchise_vs_standalone.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print('franchise_vs_standalone.png saved to reports/figures')

if __name__ == "__main__":
    with open('data/latest_timestamp.txt', 'r') as f:
        latest_timestamp = f.read().strip()

    file_path = f'data/processed/cleaned_movies_{latest_timestamp}.parquet'
    df = pd.read_parquet(file_path)
    create_visualization(df)
    print("✅ Visualizations created successfully and saved to reports/figures.")