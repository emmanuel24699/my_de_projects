"""
Functions for analyzing TMDb movie data.
Computes KPIs, filters movies, and compares franchises/directors.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import logging
from config import PROCESSED_DATA_DIR

# Setup logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rank_movies_udf(column: str, ascending: bool = False, top_n: int = 5) -> DataFrame:
    """
    UDF to rank movies based on a specified column.
    
    Args:
        column: Column to rank by (e.g., 'revenue_musd', 'vote_average').
        ascending: If True, rank in ascending order (e.g., for lowest values).
        top_n: Number of top/bottom movies to return.
    
    Returns:
        DataFrame with ranked movies.
    """
    def rank_df(df: DataFrame) -> DataFrame:
        try:
            # Filter non-null values for the ranking column
            ranked_df = (df.filter(F.col(column).isNotNull())
                        .orderBy(F.col(column).desc() if not ascending else F.col(column), 
                                 F.col("title"))  
                        .limit(top_n)
                        .select("id", "title", column))
            logger.info(f"Ranked movies by {column} (ascending={ascending}, top_n={top_n})")
            return ranked_df
        except Exception as e:
            logger.error(f"Failed to rank movies by {column}: {e}")
            raise
    return rank_df

def compute_kpis(df: DataFrame) -> dict:
    """
    Compute KPIs for best/worst performing movies and print results.
    
    Args:
        df: Spark DataFrame with movie data.
    
    Returns:
        Dictionary of DataFrames containing KPI results.
    """
    try:
        kpi_results = {}

        # 1. Highest Revenue
        kpi_results["highest_revenue"] = rank_movies_udf("revenue_musd")(df)
        logger.info("Top 5 Movies by Revenue (MUSD):")
        kpi_results["highest_revenue"].show(truncate=False)

        # 2. Highest Budget
        kpi_results["highest_budget"] = rank_movies_udf("budget_musd")(df)
        logger.info("Top 5 Movies by Budget (MUSD):")
        kpi_results["highest_budget"].show(truncate=False)

        # 3. Highest Profit
        kpi_results["highest_profit"] = rank_movies_udf("profit_musd")(df)
        logger.info("Top 5 Movies by Profit (MUSD):")
        kpi_results["highest_profit"].show(truncate=False)

        # 4. Lowest Profit
        kpi_results["lowest_profit"] = rank_movies_udf("profit_musd", ascending=True)(df)
        logger.info("Bottom 5 Movies by Profit (MUSD):")
        kpi_results["lowest_profit"].show(truncate=False)

        # 5. Highest ROI (Budget >= 10M)
        kpi_results["highest_roi"] = rank_movies_udf("roi")(df.filter(F.col("budget_musd") >= 10))
        logger.info("Top 5 Movies by ROI (Budget >= 10M):")
        kpi_results["highest_roi"].show(truncate=False)

        # 6. Lowest ROI (Budget >= 10M)
        kpi_results["lowest_roi"] = rank_movies_udf("roi", ascending=True)(df.filter(F.col("budget_musd") >= 10))
        logger.info("Bottom 5 Movies by ROI (Budget >= 10M):")
        kpi_results["lowest_roi"].show(truncate=False)

        # 7. Most Voted Movies
        kpi_results["most_voted"] = rank_movies_udf("vote_count")(df)
        logger.info("Top 5 Most Voted Movies:")
        kpi_results["most_voted"].show(truncate=False)

        # 8. Highest Rated Movies (>=10 votes)
        kpi_results["highest_rated"] = rank_movies_udf("vote_average")(df.filter(F.col("vote_count") >= 10))
        logger.info("Top 5 Highest Rated Movies (>=10 votes):")
        kpi_results["highest_rated"].show(truncate=False)

        # 9. Lowest Rated Movies (>=10 votes)
        kpi_results["lowest_rated"] = rank_movies_udf("vote_average", ascending=True)(df.filter(F.col("vote_count") >= 10))
        logger.info("Bottom 5 Lowest Rated Movies (>=10 votes):")
        kpi_results["lowest_rated"].show(truncate=False)

        # 10. Most Popular Movies
        kpi_results["most_popular"] = rank_movies_udf("popularity")(df)
        logger.info("Top 5 Most Popular Movies:")
        kpi_results["most_popular"].show(truncate=False)

        return kpi_results
    except Exception as e:
        logger.error(f"Failed to compute KPIs: {e}")
        raise

def advanced_filtering(df: DataFrame) -> dict:
    """
        Perform advanced filtering and search queries
    """
    try:
        filter_results = {}

        # Search 1: Best-rated Sci-Fi Action movies starring Bruce Willis
        sci_fi_action_willis = (df.filter(F.col("genres").like("%Science Fiction%") & 
                                         F.col("genres").like("%Action%") & 
                                         F.col("cast").like("%Bruce Willis%"))
                              .orderBy(F.col("vote_average").desc(), F.col("title"))
                              .select("title", "vote_average", "cast"))
        filter_results["sci_fi_action_willis"] = sci_fi_action_willis
        logger.info("Best-Rated Science Fiction Action Movies Starring Bruce Willis:")
        sci_fi_action_willis.show(truncate=False)

        # Search 2: Movies starring Uma Thurman, directed by Quentin Tarantino
        thurman_tarantino = (df.filter(F.col("cast").like("%Uma Thurman%") & 
                                      F.col("director").like("%Quentin Tarantino%"))
                           .orderBy(F.col("runtime").asc(), F.col("title"))
                           .select("title", "runtime", "cast", "director"))
        filter_results["thurman_tarantino"] = thurman_tarantino
        logger.info("Movies Starring Uma Thurman, Directed by Quentin Tarantino (Sorted by Runtime):")
        thurman_tarantino.show(truncate=False)

        return filter_results
    except Exception as e:
        logger.error(f"Failed to perform advanced filtering: {e}")
        raise

def franchise_vs_standalone(df: DataFrame) -> DataFrame:
    """
    Compare franchise vs. standalone movie performance.
    """
    try:
        # Categorize as franchise or standalone
        comparison_df = (df.withColumn("movie_type", 
                                      F.when(F.col("belongs_to_collection").isNotNull(), "Franchise")
                                       .otherwise("Standalone"))
                        .groupBy("movie_type")
                        .agg(
                            F.avg("revenue_musd").alias("mean_revenue_musd"),
                            F.percentile_approx("roi", 0.5).alias("median_roi"),
                            F.avg("budget_musd").alias("mean_budget_musd"),
                            F.avg("popularity").alias("mean_popularity"),
                            F.avg("vote_average").alias("mean_rating")
                        ))

        logger.info("Franchise vs. Standalone Movie Performance:")
        comparison_df.show(truncate=False)

        return comparison_df
    except Exception as e:
        logger.error(f"Failed to compare franchise vs. standalone: {e}")
        raise

def analyze_franchises(df: DataFrame) -> DataFrame:
    """
    Identify the most successful movie franchises, and print results.
    """
    try:
        franchise_df = (df.filter(F.col("belongs_to_collection").isNotNull())
                       .groupBy("belongs_to_collection")
                       .agg(
                           F.count("*").alias("movie_count"),
                           F.sum("budget_musd").alias("total_budget_musd"),
                           F.avg("budget_musd").alias("mean_budget_musd"),
                           F.sum("revenue_musd").alias("total_revenue_musd"),
                           F.avg("revenue_musd").alias("mean_revenue_musd"),
                           F.avg("vote_average").alias("mean_rating")
                       )
                       .orderBy(F.col("total_revenue_musd").desc()))

        logger.info("Most Successful Movie Franchises:")
        franchise_df.show(truncate=False)

        return franchise_df
    except Exception as e:
        logger.error(f"Failed to analyze franchises: {e}")
        raise

def analyze_directors(df: DataFrame) -> DataFrame:
    """
    Identify the most successful directors, and print results.
    """
    try:
        # Split directors (in case multiple directors are listed)
        director_df = (df.withColumn("director", F.explode(F.split("director", " \| ")))
                      .groupBy("director")
                      .agg(
                          F.count("*").alias("movie_count"),
                          F.sum("revenue_musd").alias("total_revenue_musd"),
                          F.avg("vote_average").alias("mean_rating")
                      )
                      .orderBy(F.col("total_revenue_musd").desc()))

        logger.info("Most Successful Directors:")
        director_df.show(truncate=False)

        return director_df
    except Exception as e:
        logger.error(f"Failed to analyze directors: {e}")
        raise

def analyze_data(df: DataFrame, output_dir: str = PROCESSED_DATA_DIR) -> tuple:
    """
    Orchestrate all analysis tasks and return updated DataFrame.
    """
    try:
        results = {}

        # Add profit_musd and roi to the DataFrame
        df = df.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
        df = df.withColumn("roi", F.when(F.col("budget_musd") >= 10, 
                                F.col("revenue_musd") / F.col("budget_musd"))
                          .otherwise(None).cast(FloatType()))

        # Compute KPIs and print results
        logger.info("Computing KPIs")
        results["kpis"] = compute_kpis(df)

        # Perform advanced filtering
        logger.info("Performing advanced filtering")
        results["filters"] = advanced_filtering(df)

        # Compare franchise vs. standalone
        logger.info("Comparing franchise vs. standalone")
        results["franchise_vs_standalone"] = franchise_vs_standalone(df)

        # Analyze franchises
        logger.info("Analyzing franchises")
        results["franchises"] = analyze_franchises(df)

        # Analyze directors
        logger.info("Analyzing directors")
        results["directors"] = analyze_directors(df)

        logger.info("All analysis tasks completed")
        return df, results
    except Exception as e:
        logger.error(f"Failed to analyze data: {e}")
        raise