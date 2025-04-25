# TMDB Movie Data Analysis with Apache Spark

This project is a data engineering pipeline that fetches, cleans, analyzes, and visualizes movie data from The Movie Database (TMDB) API using Apache Spark. It processes a predefined list of movie IDs, cleans the data, computes key performance indicators (KPIs), performs advanced filtering, compares franchises and directors, and generates visualizations to explore trends and insights.

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Module Descriptions](#module-descriptions)
- [File Structure](#file-structure)
- [Outputs](#outputs)
- [Dependencies](#dependencies)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Project Overview

The TMDB Movie Data Analysis pipeline leverages Apache Spark for data processing and Python for API interactions and visualization. It fetches movie metadata (e.g., budget, revenue, genres) from the TMDB API, cleans and preprocesses the data, computes KPIs (e.g., highest revenue, ROI), compares franchise vs. standalone movies, and visualizes trends (e.g., revenue vs. budget, ROI by genre).

The pipeline is modular, with all helper functions consolidated in `scripts/helpers.py` and orchestration in `scripts/pipeline.py`. A Jupyter notebook (`notebooks/TMDB Movie Analysis.ipynb`) provides an interactive interface for exploring the pipeline. Outputs include raw and processed data files, visualization plots, and console outputs for inspection and analysis results.

## Features

- **Data Fetching**: Retrieves movie data for specified IDs from the TMDB API, caching results as JSON.
- **Data Cleaning**: Processes JSON fields, converts data types, removes duplicates, and filters invalid rows using Spark.
- **Analysis**:
  - Computes KPIs (e.g., top 5 movies by revenue, ROI, rating).
  - Compares franchise vs. standalone movie performance.
  - Analyzes top franchises and directors by revenue and rating.
- **Visualization**: Generates plots for revenue vs. budget, ROI by genre, popularity vs. rating, yearly trends, and franchise vs. standalone comparisons.
- **Interactive Exploration**: Jupyter notebook for step-by-step analysis and visualization.

## Prerequisites

- **Python**: Version 3.8 or higher.
- **Apache Spark**: Version 3.2.0 or compatible.
- **TMDB API Key**: Obtain from [TMDB](https://www.themoviedb.org/settings/api).
- **Java**: Required for Spark (Java 8 or 11 recommended).
- **Jupyter**: For running the notebook.
- **Git**: For cloning the repository.
- **Internet Connection**: For API requests and dependency installation.

## Setup Instructions

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/emmanuel24699/my_de_projects.git
   cd spark_tmdb_analysis
   ```

2. **Set Up a Virtual Environment** (optional but recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:
   Install:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure TMDB API Key**:
   Create a `.env` file in the project root:

   ```
   TMDB_API_KEY=your-api-key-here
   ```

   Replace `your-api-key-here` with your TMDB API key.

5. **Verify Spark Installation**:
   Ensure Spark is installed and accessible:
   ```bash
   spark-submit --version
   ```
   If not installed, download from [Apache Spark](https://spark.apache.org/downloads.html) and set up environment variables (`SPARK_HOME`, `PATH`).

## Usage

### Run the Pipeline

Execute the automated pipeline to fetch, clean, analyze, and visualize TMDB movie data:

```bash
python scripts/pipeline.py
```

### Use the Notebook

Launch the Jupyter notebook for interactive exploration:

```bash
jupyter notebook notebooks/TMDB Movie Analysis.ipynb
```

- Follow the notebook cells to fetch data, clean it, analyze KPIs, and visualize results.
- Outputs (e.g., tables, plots) are displayed inline.
- Plots are also saved to `reports/figures`.

### Steps Performed

1. **Initialize Spark**: Sets up a Spark session with custom configurations.
2. **Fetch Data**: Retrieves movie data for predefined IDs from TMDB API, caching as JSON in `data/raw`.
3. **Clean Data**: Processes and cleans the data, saving as Parquet in `data/processed`.
4. **Analyze Data**: Computes KPIs, filters movies, and analyzes franchises/directors.
5. **Visualize Data**: Generates five plots saved in `reports/figures`.

### Expected Outputs

- **Data Files**:
  - Raw JSON: `data/raw/raw_movies_<timestamp>.json`
  - Cleaned Parquet: `data/processed/cleaned_movies_<timestamp>.parquet`
- **Visualizations** (PNG files in `reports/figures`):
  - `revenue_vs_budget.png`: Scatter plot of revenue vs. budget.
  - `roi_by_genre.png`: Box plot of ROI by genre.
  - `popularity_vs_rating.png`: Scatter plot of popularity vs. rating.
  - `yearly_trends.png`: Line plot of yearly revenue and budget trends.
  - `franchise_vs_standalone.png`: Bar plot comparing franchise vs. standalone metrics.
- **Console Outputs** (or notebook outputs):
  - Inspection tables for columns (e.g., genres, directors) using `show(truncate=False)`.
  - KPI results (e.g., top 5 movies by revenue, ROI).
  - Advanced filter results (e.g., Bruce Willis Sci-Fi Action movies).
  - Franchise and director analysis tables.

## Module Descriptions

### `scripts/helpers.py`

Contains all helper functions for the pipeline, organized into sections:

- **Configuration**: Defines constants (`TMDB_API_KEY`, `MOVIE_IDS`, `RAW_DATA_DIR`, etc.) and TMDb schema.
- **Data Fetching**:
  - `initialize_spark`: Initializes a Spark session.
  - `fetch_movie_data`: Fetches data for a single movie ID from TMDB API.
  - `load_cached_data`: Loads cached JSON data.
  - `create_movie_dataframe`: Creates a Spark DataFrame from fetched data.
- **Data Cleaning**:
  - `clean_data`: Orchestrates cleaning steps (dropping columns, processing JSON, converting types, etc.).
  - `inspect_column`: Displays value counts for a column.
  - Other functions: `drop_irrelevant_columns`, `process_json_fields`, `convert_data_types`, etc.
- **Analysis**:
  - `analyze_data`: Orchestrates analysis tasks.
  - `compute_kpis`: Computes KPIs (e.g., highest revenue, ROI).
  - `advanced_filtering`: Filters movies (e.g., Sci-Fi with Bruce Willis).
  - `franchise_vs_standalone`, `analyze_franchises`, `analyze_directors`: Compare and rank movies.
- **Visualization**:
  - `visualize_data`: Orchestrates plot generation.
  - Individual plot functions: `plot_revenue_vs_budget`, `plot_roi_by_genre`, etc.

### `scripts/pipeline.py`

Orchestrates the pipeline:

- **Main Function**: `run_pipeline`
- **Steps**:
  1. Initializes Spark.
  2. Fetches and creates a raw DataFrame.
  3. Cleans the data.
  4. Analyzes the data.
  5. Generates visualizations.
- **Imports**: Functions and constants from `helpers.py`.

### `notebooks/TMDB Movie Analysis.ipynb`

Provides an interactive interface for the pipeline:

- **Purpose**: Allows step-by-step exploration of data fetching, cleaning, analysis, and visualization.
- **Content**:
  - Imports `helpers.py` functions.
  - Initializes Spark and fetches data.
  - Cleans data and displays schema.
  - Computes KPIs and filters movies.
  - Generates and displays visualizations inline.
- **Usage**: Run in Jupyter with `jupyter notebook notebooks/TMDB Movie Analysis.ipynb`.

## File Structure

```
spark_tmdb_analysis/
├── scripts/
│   ├── helpers.py
│   ├── pipeline.py
├── notebooks/
│   ├── TMDB Movie Analysis.ipynb
├── data/
│   ├── raw/
│   ├── processed/
├── reports/
│   ├── figures/
├── .env
├── requirements.txt
├── README.md
```

## Outputs

### Data Files

- **Raw Data**: `data/raw/raw_movies_<timestamp>.json`
  - Contains fetched movie data for specified IDs.
  - Timestamped to track runs.
- **Processed Data**: `data/processed/cleaned_movies_<timestamp>.parquet`
  - Cleaned and preprocessed movie data in Parquet format.
  - Includes columns like `id`, `title`, `budget_musd`, `genres`, `cast`, etc.

### Visualizations

Saved in `reports/figures`:

1. `revenue_vs_budget.png`: Scatter plot showing revenue vs. budget.
2. `roi_by_genre.png`: Box plot of ROI distribution across genres.
3. `popularity_vs_rating.png`: Scatter plot of popularity vs. average rating.
4. `yearly_trends.png`: Line plot of average revenue and budget over years.
5. `franchise_vs_standalone.png`: Bar plot comparing franchise and standalone movie metrics.

### Console/Notebook Outputs

- **Inspection Tables**: Value counts for columns (e.g., genres, directors) displayed as Spark DataFrame tables.
- **KPIs**:
  - Top 5 movies by revenue, budget, profit, ROI, votes, rating, popularity.
  - Bottom 5 by profit, ROI, rating.
- **Advanced Filters**:
  - Sci-Fi Action movies starring Bruce Willis, sorted by rating.
  - Uma Thurman movies directed by Quentin Tarantino, sorted by runtime.
- **Analyses**:
  - Franchise vs. standalone: Mean revenue, ROI, rating, etc.
  - Top franchises: Total revenue, movie count, mean rating.
  - Top directors: Total revenue, movie count, mean rating.

## Dependencies

Install via `requirements.txt`:

```
pip install -r requirements.txt
```

Additional requirements:

- **Java 8 or 11**: For Spark.
- **Spark 3.2.0**: Configured via `SPARK_HOME`.

## Troubleshooting

- **TMDB API Key Issues**:
  - Ensure `.env` contains a valid `TMDB_API_KEY`.
  - Verify API key on [TMDB](https://www.themoviedb.org/settings/api).
- **Spark Errors**:
  - Check Java installation: `java -version`.
  - Ensure `SPARK_HOME` is set and Spark is accessible.
- **API Rate Limits**:
  - The pipeline includes retries with exponential backoff. If issues persist, reduce `MOVIE_IDS` or add delays.
- **Missing Outputs**:
  - Verify directory permissions for `data/` and `reports/`.
  - Check logs in console for errors.
- **Visualization Issues**:
  - Ensure `matplotlib`, `seaborn`, and `jupyter` are installed.
  - If plots don’t display in the notebook, ensure `%matplotlib inline` is set.
- **Notebook Path Issues**:
  - Ensure `scripts/` is in the Python path by running `sys.path.append(os.path.abspath('../scripts'))`.

## Contributing

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature/your-feature`.
3. Commit changes: `git commit -m "Add your feature"`.
4. Push to the branch: `git push origin feature/your-feature`.
5. Open a pull request with a detailed description.

### Suggested Improvements

- Expand `MOVIE_IDS` or fetch dynamically.
- Add unit tests for `helpers.py` functions.
- Implement rate limiting for TMDB API.
- Add more visualizations (e.g., cast popularity trends).
- Enhance the notebook with custom queries.

---
