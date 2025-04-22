"""
Functions for analyzing the cleaned TMDb movie data as per project requirements.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pathlib import Path
import logging
from config import PROCESSED_DATA_DIR
from data_cleaning import clean_data
from api_fetch import create_movie_dataframe, initialize_spark

# Setup logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rank_movies_udf(metric_col: str, ascending: bool = False, limit: int = 5):
    """
    UDF to rank movies based on a metric column.
    
    Args:
        metric_col: Column to rank by (e.g., 'revenue_musd').
        ascending: True for ascending order (lowest), False for descending (highest).
        limit: Number of movies to return.
    
    Returns:
        Function that applies ranking to a DataFrame.
    """
    def rank(df: DataFrame) -> DataFrame:
        return (df
                .filter(F.col(metric_col).isNotNull())
                .select("id", "title", metric_col)
                .orderBy(F.col(metric_col).desc() if not ascending else F.col(metric_col))
                .limit(limit))
    return rank

def analyze_data(df: DataFrame, output_dir: str = PROCESSED_DATA_DIR) -> DataFrame:
    """
    Analyze the cleaned TMDb movie DataFrame to compute KPIs and perform queries.
    
    Args:
        df: Cleaned Spark DataFrame.
        output_dir: Directory to save analysis results.
    
    Returns:
        Spark DataFrame with all KPI results.
    """
    try:
        # Compute profit and ROI
        df = df.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
        df = df.withColumn("roi", F.when(F.col("budget_musd") >= 10,
                                         F.col("revenue_musd") / F.col("budget_musd"))
                                    .otherwise(None).cast(FloatType()))
        
        # Best/Wort Performing Movies
        logger.info("Computing best/worst performing movies...")
        highest_revenue = rank_movies_udf("revenue_musd")(df).withColumn("kpi", F.lit("highest_revenue"))
        highest_budget = rank_movies_udf("budget_musd")(df).withColumn("kpi", F.lit("highest_budget"))
        highest_profit = rank_movies_udf("profit_musd")(df).withColumn("kpi", F.lit("highest_profit"))
        lowest_profit = rank_movies_udf("profit_musd", ascending=True)(df).withColumn("kpi", F.lit("lowest_profit"))
        highest_roi = rank_movies_udf("roi")(df).withColumn("kpi", F.lit("highest_roi"))
        lowest_roi = rank_movies_udf("roi", ascending=True)(df).withColumn("kpi", F.lit("lowest_roi"))
        most_voted = rank_movies_udf("vote_count")(df).withColumn("kpi", F.lit("most_voted"))
        highest_rated = (df
                         .filter(F.col("vote_count") >= 10)
                         .select("id", "title", "vote_average")
                         .orderBy(F.col("vote_average").desc())
                         .limit(5)
                         .withColumn("kpi", F.lit("highest_rated")))
        lowest_rated = (df
                        .filter(F.col("vote_count") >= 10)
                        .select("id", "title", "vote_average")
                        .orderBy(F.col("vote_average"))
                        .limit(5)
                        .withColumn("kpi", F.lit("lowest_rated")))
        most_popular = rank_movies_udf("popularity")(df).withColumn("kpi", F.lit("most_popular"))

        # Advanced Filtering & Search Queries
        logger.info("Computing advanced filtering and search queries...")
        # best-rated Science Fiction Action movies starring Bruce Willis (sorted by Rating - highest to lowest)
        sci_fi_action_bruce = (df
            .filter(F.col("genres").like("%Science Fiction%") & F.col("genres").like("%Action%") & 
                    F.col("cast").like("%Bruce Willis%"))
            .select("id", "title", "vote_average")
            .orderBy(F.col("vote_average").desc())
            .withColumn("kpi", F.lit("sci_fi_action_bruce_willis")))
        
        # Movies starng Uma Thurman, directed by Quentin Tarantino (sorted by runtime- shortest to longest)
        uma_tarantino = (df
            .filter(F.col("cast").like("%Uma Thurman%") & F.col("director").like("%Quentin Tarantino%"))
            .select("id", "title", "runtime")
            .orderBy(F.col("runtime"))
            .withColumn("kpi", F.lit("uma_thurman")))
        
        # 3. Franchise vs. Standalone Movie Performance
        logger.info("Comparing franchise vs. standalone movies")
        franchise_stats = (df
            .withColumn("is_franchise", F.when(F.col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone"))
            .groupBy("is_franchise")
            .agg(
                F.avg("revenue_musd").alias("mean_revenue_musd"),
                F.expr("percentile_approx(roi, 0.5)").alias("median_roi"),
                F.avg("budget_musd").alias("mean_budget_musd"),
                F.avg("popularity").alias("mean_popularity"),
                F.avg("vote_average").alias("mean_rating")
            )
            .withColumn("kpi", F.lit("franchise_vs_standalone")))
        
        # Most Successful Franchise
        logger.info("Identifying most successful franchises")
        franchise_performance = (df
            .filter(F.col("belongs_to_collection").isNotNull())
            .withColumn("franchise_name", F.col("belongs_to_collection.name"))
            .groupBy("franchise_name")
            .agg(
                F.count("*").alias("movie_count"),
                F.sum("budget_musd").alias("total_budget_musd"),
                F.avg("budget_musd").alias("mean_budget_musd"),
                F.sum("revenue_musd").alias("total_revenue_musd"),
                F.avg("revenue_musd").alias("mean_revenue_musd"),
                F.avg("vote_average").alias("mean_rating")
            )
            .orderBy(F.col("total_revenue_musd").desc())
            .limit(5)
            .withColumn("kpi", F.lit("successful_franchises")))
        
        # Most Successful Directors
        logger.info("Identifying most successful directors")
        director_performance = (df
            .withColumn("director_name", F.explode(F.split(F.col("director"), "\\|")))
            .groupBy("director_name")
            .agg(
                F.count("*").alias("movie_count"),
                F.sum("revenue_musd").alias("total_revenue_musd"),
                F.avg("vote_average").alias("mean_rating")
            )
            .orderBy(F.col("total_revenue_musd").desc())
            .limit(5)
            .withColumn("kpi", F.lit("successful_directors")))
        
        # Combine al results into a single DataFrame
        logger.info("Combining all results into a single DataFrame")
        results = (highest_revenue
            .union(highest_budget)
            .union(highest_profit)
            .union(lowest_profit)
            .union(highest_roi)
            .union(lowest_roi)
            .union(most_voted)
            .union(highest_rated)
            .union(lowest_rated)
            .union(most_popular)
            .union(sci_fi_action_bruce)
            .union(uma_tarantino)
            .union(franchise_stats.select(
                "kpi", F.col("is_franchise").alias("metric1"), 
                F.col("mean_revenue_musd").alias("metric2"), 
                F.col("median_roi").alias("metric3"), 
                F.col("mean_budget_musd").alias("metric4"), 
                F.col("mean_popularity").alias("metric5"), 
                F.col("mean_rating").alias("metric6")))
            .union(franchise_performance.select(
                "kpi", F.col("belongs_to_collection").alias("metric1"), 
                F.col("movie_count").alias("metric2"), 
                F.col("total_budget_musd").alias("metric3"), 
                F.col("mean_budget_musd").alias("metric4"), 
                F.col("total_revenue_musd").alias("metric5"), 
                F.col("mean_revenue_musd").alias("metric6"), 
                F.col("mean_rating").alias("metric7")))
            .union(director_performance.select(
                "kpi", F.col("director_name").alias("metric1"), 
                F.col("movie_count").alias("metric2"), 
                F.col("total_revenue_musd").alias("metric3"), 
                F.col("mean_rating").alias("metric4")))
        )

        # Save result as a Parquet file
        logger.info("Saving results to Parquet file")
        output_path = Path(output_dir) / "kpi_results.parquet"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        results.write.mode("overwrite").parquet(str(output_path))
        logger.info(f"Analysis results saved to {output_path}")

        return results
    
    except Exception as e:
        logger.error(f"Error in analyze_data: {e}")
        raise
