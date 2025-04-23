# TMDB Movie Data Analysis

This project analyzes movie data from The Movie Database (TMDb) using Python, Pandas, and APIs. 
It includes data extraction, cleaning, KPI analysis, and visualizations.

## Structure
- `data/`: Raw and processed data
- `scripts/`: Reusable Python functions
- `notebooks/`: Jupyter notebooks per phase
- `reports/`: Graphs and figures


---

# TMDB Movie Data Analysis Project

## Project Overview

This project implements a movie data analysis pipeline using Python, Pandas, and the TMDb (The Movie Database) API. The pipeline fetches movie data, cleans and transforms it, performs exploratory data analysis (EDA), calculates key performance indicators (KPIs), conducts advanced filtering and ranking, analyzes franchise and director performance, and visualizes key insights. The project is executed independently, showcasing a modular workflow from data extraction to visualization.

---

## Objectives

- **API Data Extraction**: Retrieve movie metadata from TMDb for a specified list of movie IDs.
- **Data Cleaning & Transformation**: Process raw API data into a structured, analysis-ready format.
- **Exploratory Data Analysis (EDA)**: Explore trends and patterns in the dataset.
- **Advanced Filtering & Ranking**: Identify top and bottom movies based on financial and popularity metrics.
- **Franchise & Director Analysis**: Evaluate performance of movie franchises and directors.
- **Visualization & Insights**: Present findings through graphical representations.

---

## Project Structure

```
tmdb_movie_analysis/
├── src/
│   ├── data_fetching.py  # Extract raw data from API
│   ├── data_cleaning.py  # Data preprocessing
│   ├── analysis.py       # KPI calculations and analysis
│   ├── visualization.py  # Data visualization
│   ├── main.py           # Pipeline orchestration
│   └── config.py         # Configuration (API key, base URL, movie IDs)
├── data/
│   ├── raw/
│   │   └── movies_{timestamp}.json  # Raw API data
│   ├── processed/
│   │   ├── cleaned_movies_{timestamp}.parquet  # Cleaned data
│   │   └── analyzed_movies_{timestamp}.parquet  # Analyzed data
│   └── latest_timestamp.txt  # Latest run timestamp
├── reports/
│   └── figures/
│       ├── revenue_vs_budget.png
│       ├── roi_by_genre.png
│       ├── popularity_vs_rating.png
│       ├── yearly_trends.png
│       └── franchise_vs_standalone.png
├── README.md  # Project documentation
└── notebooks/
```

---

## Project Steps

### Step 1: Fetch Movie Data from API

- **API**: TMDb (The Movie Database).
- **Movie IDs**: `[0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]`.
- **Implementation**: `data_fetching.py` uses the `requests` library to fetch data, including credits (cast and crew), and saves it as a JSON file (`movies_{timestamp}.json`).
- **Output**: Pandas DataFrame stored as JSON with a timestamp recorded in `latest_timestamp.txt`.

### Step 2: Data Cleaning and Preprocessing

- **Script**: `data_cleaning.py`
- **Actions**:
  1. **Drop Columns**: Removed `['adult', 'imdb_id', 'original_title', 'video', 'homepage']`.
  2. **Parse JSON Columns**:
     - `belongs_to_collection`: Extracted collection name or NaN.
     - `genres`, `production_countries`, `production_companies`, `spoken_languages`: Converted to strings with "|" separator.
     - Added `cast`, `cast_size`, `director`, `crew_size` from `credits`.
  3. **Data Type Conversion**:
     - `budget`, `revenue` → Numeric (converted to million USD).
     - `release_date` → Datetime.
     - `id`, `popularity`, etc. → Appropriate types (e.g., int, float, category).
  4. **Handle Missing/Incorrect Data**:
     - Replaced 0 with NaN for `budget`, `revenue`, `runtime`.
     - Set `vote_average` to NaN if `vote_count` = 0.
     - Replaced empty strings or "No Data" with NaN in `overview`, `tagline`.
     - Removed duplicates and rows with missing `id` or `title`.
     - Kept rows with ≥10 non-NaN values.
     - Filtered for `Released` movies and dropped `status`.
  5. **Finalize**:
     - Reordered columns as specified.
     - Reset index.
- **Output**: Cleaned DataFrame saved as `cleaned_movies_{timestamp}.parquet`.

### Step 3: KPI Implementation & Analysis

- **Script**: `analysis.py`
- **Actions**:
  1. **KPIs**:
     - Added `profit_musd` (revenue - budget) and `roi` (revenue / budget).
     - Ranked movies using a function (`rank_movies`):
       - Highest/Lowest Revenue, Budget, Profit, ROI (Budget ≥ 10M), Votes, Rating (Votes ≥ 10), Popularity.
  2. **Advanced Filtering**:
     - Sci-Fi Action movies with Bruce Willis, sorted by rating.
     - Uma Thurman movies directed by Quentin Tarantino, sorted by runtime.
  3. **Franchise vs. Standalone**:
     - Compared mean revenue, median ROI, mean budget, mean popularity, and mean rating.
  4. **Franchise Analysis**:
     - Aggregated by `belongs_to_collection` for number of movies, total/mean budget, total/mean revenue, mean rating.
  5. **Director Analysis**:
     - Aggregated by `director` for number of movies, total revenue, mean rating.
- **Output**: Dictionary of results; updated DataFrame saved as `cleaned_movies_{timestamp}.parquet`.

### Step 4: Data Visualization

- **Script**: `visualization.py`
- **Visualizations**:
  1. **Revenue vs. Budget Trends**: Scatter plot of budget vs. revenue.
  2. **ROI Distribution by Genre**: Box plot for top 10 genres.
  3. **Popularity vs. Rating**: Scatter plot of popularity vs. vote average.
  4. **Yearly Trends**: Line plot of total revenue and budget by year.
  5. **Franchise vs. Standalone**: Bar plot comparing mean metrics.
- **Output**: PNG files saved in `reports/figures/`.

---

## Deliverables

### Complete Workflow

- **Script**: `src/pipeline.py`
- **Description**: Orchestrates the pipeline:
  1. Fetches data.
  2. Cleans and saves it.
  3. Analyzes and saves updated data.
  4. Generates visualizations.
- **Execution**:
  ```bash
  cd tmdb_movie_analysis
  py src/pipeline.py
  ```

### Final Report

- **Key Insights**:
  - Revenue for movies has in recent years has become relatively higher even though there has not been substantial increase in budgets.
  - High ROI movies tend to have lower budgets.
  - Popularity correlates weakly with ratings.
  - Standalone movies outperform Franchise movies in terms of average revenue and ROI.
  - 
- **Methodology**: API extraction → Cleaning → KPI calculation → Visualization.


---

## Dependencies

- Python 3.x
- Libraries:
  ```bash
  pip install requirements.txt
  ```

---

## Configuration

- **`.env`**:
  ```python
  TMDB_API_KEY = "your_api_key_here"
  ```
- **`src/config.py`**:
  ```python
  BASE_URL = "https://api.themoviedb.org/3/movie"
  MOVIE_IDS = [0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]
  ```

---

## Usage

1. Clone the repository:
   ```bash
   git clone <repository_url>
   cd tmdb_movie_analysis
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Add `.env` with your TMDb API key.
4. Run the pipeline:
   ```bash
   py src/pipeline.py
   ```

---
