"""
Functions for visualizing TMDb movie data using Matplotlib.
Generates plots for revenue, ROI, popularity, yearly trends, and franchise comparisons.
"""
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pathlib import Path
import logging
from config import FIGURES_DIR

# Setup logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def plot_revenue_vs_budget(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """ Plot a scatter of revenue vs budget"""
    try:
        # Convert to Pandas, filter non-null values
        df_pd = df.select("title", "budget_musd","revenue_musd") \
                .filter(F.col("budget_musd").isNotNull() & F.col("revenue_musd").isNotNull()) \
                .toPandas()
        
        plt.figure(figsize=(10,10))
        sns.scatterplot(x="budget_musd", y="revenue_musd", data=df_pd, alpha=0.6)
        plt.title("Revenue vs Budget")
        plt.xlabel("Budget (MUSD)")
        plt.ylabel("Revenue (MUSD)")
        plt.grid(True)

        output_path = Path(output_dir) / "revenue_vs_budget.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved revenue vs. budget plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot revenue vs. budget: {e}")
        raise

def plot_roi_by_genre(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Plot a box plot of ROI distribution by genre.
    """
    try:
        # Explode genres and filter non-null ROI
        df_exploded = df.select("roi", F.explode(F.split("genres", " \| ")).alias("genres")) \
                        .filter(F.col("roi").isNotNull()) 
        
        df_pd = df_exploded.toPandas()

        plt.figure(figsize=(12, 8))
        sns.boxplot(x="genres", y="roi", data=df_pd)
        plt.title("ROI Distribution by Genre")
        plt.xlabel("Genre")
        plt.ylabel("ROI (Revenue/Budget)")
        plt.xticks(rotation=45, ha="right")
        plt.grid(True, axis="y")

        output_path = Path(output_dir) / "roi_by_genre.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved ROI by genre plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot ROI by genre: {e}")
        raise

def plot_popularity_vs_rating(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Plot a scatter of ppularity vs. rating"""
    try:
        # Convert to pandas, filter non-null values
        df_pd = df.select("title", "popularity", "vote_average") \
                    .filter(F.col("popularity").isNotNull() & F.col("vote_average").isNotNull()) \
                    .toPandas()
        
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x="popularity", y="vote_average", data=df_pd, alpha=0.6)
        plt.title("Popularity vs. Rating")
        plt.xlabel("Popularity")
        plt.ylabel("Average Rating")
        plt.grid(True)
        
        output_path = Path(output_dir) / "popularity_vs_rating.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved popularity vs. rating plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot popularity vs. rating: {e}")
        raise

def plot_yearly_trends(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """ Plot yearly trends in average revenue and budget"""
    try:
        # Extract year and coompute averages
        df_yearly = df.withColumn("year", F.year("release_date")) \
                        .filter(F.col("year").isNotNull())\
                        .groupby("year")\
                        .agg(
                            F.avg("revenue_musd").alias("avg_revenue_musd"),
                            F.avg("budget_musd").alias("avg_budget_musd")) \
                        .orderBy("year") \
                        .toPandas()
        plt.figure(figsize=(12, 6))
        plt.plot(df_yearly["year"], df_yearly["avg_revenue_musd"], label="Average Revenue (MUSD)", marker="o")
        plt.plot(df_yearly["year"], df_yearly["avg_budget_musd"], label="Average Budget (MUSD)", marker="s")
        plt.title("Yearly Trends in Box Office Performance")
        plt.xlabel("Year")
        plt.ylabel("Amount (MUSD)")
        plt.legend()
        plt.grid(True)
        
        output_path = Path(output_dir) / "yearly_trends.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved yearly trends plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot yearly trends: {e}")
        raise

def plot_franchise_vs_standalone(df:DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """Plot a bar comparison of franchise vs. sandalone movie success"""
    try:
        # Compute metrics
        comparison_df = (df.withColumn("movie_type",
                                       F.when(F.col("belongs_to_collection").isNotNull(),  "Franchise")
                                       .otherwise("Standalone"))
                            .groupBy("movie_type")
                            .agg(
                                F.avg("revenue_musd").alias("mean_revenue_musd"),
                                F.avg("roi").alias("mean_roi"),
                                F.avg("vote_average").alias("mean_rating")
                            )\
                            .toPandas())
        # Prepare data for plotting
        metrics = ["mean_revenue_musd", "mean_roi", "mean_rating"]
        labels = ["Revenue (MUSD)", "ROI", "Rating"]
        franchise_data = comparison_df[comparison_df["movie_type"] == "Franchise"][metrics].iloc[0]
        standalone_data = comparison_df[comparison_df["movie_type"] == "Standalone"][metrics].iloc[0]

        x = range(len(metrics))
        width = 0.35

        plt.figure(figsize=(10, 6))
        plt.bar([i - width/2 for i in x], franchise_data, width, label="Franchise", color="skyblue")
        plt.bar([i + width/2 for i in x], standalone_data, width, label="Standalone", color="lightcoral")
        plt.title("Franchise vs. Standalone Movie Success")
        plt.xticks(x, labels)
        plt.ylabel("Value")
        plt.legend()
        plt.grid(True, axis="y")
        
        output_path = Path(output_dir) / "franchise_vs_standalone.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        logger.info(f"Saved franchise vs. standalone plot to {output_path}")
    except Exception as e:
        logger.error(f"Failed to plot franchise vs. standalone: {e}")
        raise

def visualize_data(df: DataFrame, output_dir: str = FIGURES_DIR) -> None:
    """
    Orchestrate all visualization tasks.
    
    Args:
        df: Spark DataFrame with movie data (must include required columns).
        output_dir: Directory to save plots.
    """
    try:
        logger.info("Generating visualization plots")

        # Generate each plot 
        plot_revenue_vs_budget(df, output_dir)
        plot_roi_by_genre(df, output_dir)
        plot_popularity_vs_rating(df, output_dir)
        plot_franchise_vs_standalone(df, output_dir)
        plot_yearly_trends(df, output_dir)

        logger.info("All visualizations completed")
    except Exception as e:
        logger.error(f"Failed to generate visualizations: {e}")
        raise