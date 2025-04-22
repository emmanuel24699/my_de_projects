import pandas as pd
import numpy as np
import os
from datetime import datetime
from config import TMDB_API_KEY, BASE_URL, MOVIE_IDS, RAW_DATA_DIR, PROCESSED_DATA_DIR
from pathlib import Path


def drop_irrelevant_columns(df, columns_to_drop=None):
    """Drop specified columns from the DataFrame, ignoring errors."""
    df = df.copy()  # Ensure no modification to input
    if columns_to_drop is None:
        columns_to_drop = ['adult', 'backdrop_path', 'imdb_id', 'original_title', 'video', 'homepage']
    return df.drop(columns=columns_to_drop, errors='ignore')


def evaluate_json_columns(df):
    """Process JSON-like columns into readable formats."""
    df = df.copy()
    required_columns = ['belongs_to_collection', 'genres', 'spoken_languages', 
                       'production_countries', 'production_companies', 'origin_country', 'credits']
    
    # Check for required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing columns {missing_cols}. JSON evaluation may be incomplete.")
    
    try:
        df['belongs_to_collection'] = df['belongs_to_collection'].apply(
            lambda x: x['name'] if isinstance(x, dict) and 'name' in x else np.nan
        )
        df['genres'] = df['genres'].apply(
            lambda x: " | ".join([g['name'] for g in x]) if isinstance(x, list) else np.nan
        )
        df['spoken_languages'] = df['spoken_languages'].apply(
            lambda x: " | ".join([l['iso_639_1'] for l in x]) if isinstance(x, list) else np.nan
        )
        df['production_countries'] = df['production_countries'].apply(
            lambda x: " | ".join([c['iso_3166_1'] for c in x]) if isinstance(x, list) else np.nan
        )
        df['production_companies'] = df['production_companies'].apply(
            lambda x: " | ".join([c['name'] for c in x]) if isinstance(x, list) else np.nan
        )
        df['origin_country'] = df['origin_country'].apply(
            lambda x: " | ".join(sorted(x)) if isinstance(x, list) else np.nan
        )
        if 'credits' in df.columns:
            df["cast"] = df["credits"].apply(
                lambda x: " | ".join([person["name"] for person in x.get("cast", [])])
            )
            df["cast_size"] = df["credits"].apply(
                lambda x: len(x.get("cast", []))
            )
            df["director"] = df["credits"].apply(
                lambda x: " | ".join([person["name"] for person in x.get("crew", []) if person.get("job") == "Director"])
            )
            df["crew_size"] = df["credits"].apply(
                lambda x: len(x.get("crew", []))
            )
            df = df.drop(columns=['credits'], errors='ignore')
    except Exception as e:
        print(f"Error processing JSON columns: {e}")
    return df


def inspect_extracted_columns(df, columns_to_inspect=None):
    """Inspect the distribution of specified columns using value_counts."""
    if columns_to_inspect is None:
        columns_to_inspect = [
            'belongs_to_collection', 'genres', 'spoken_languages', 'production_countries',
            'cast', 'cast_size', 'director', 'crew_size', 'production_companies'
        ]
    for col in columns_to_inspect:
        if col in df.columns:
            print(f"\n{col.replace('_', ' ').title()} distribution:")
            print(df[col].value_counts(dropna=False).head(10))  # Limit to top 10 for brevity
        else:
            print(f"Warning: Column {col} not found in DataFrame.")


def convert_datatypes(df):
    """Convert DataFrame columns to appropriate datatypes."""
    df = df.copy()
    conversions = {
        'budget': (pd.to_numeric, {'errors': 'coerce'}, lambda x: x / 1000000),
        'revenue': (pd.to_numeric, {'errors': 'coerce'}, lambda x: x / 1000000),
        'popularity': (pd.to_numeric, {'errors': 'coerce'}, None),
        'release_date': (pd.to_datetime, {'errors': 'coerce'}, None),
        'belongs_to_collection': (lambda x: x.astype('category'), {}, None),
        'genres': (lambda x: x.astype('category'), {}, None),
        'id': (lambda x: x.astype('int64'), {}, None),
        'original_language': (lambda x: x.astype('category'), {}, None),
        'origin_country': (lambda x: x.astype('category'), {}, None),
        'overview': (lambda x: x.astype('string'), {}, None),
        'poster_path': (lambda x: x.astype('string'), {}, None),
        'production_companies': (lambda x: x.astype('category'), {}, None),
        'production_countries': (lambda x: x.astype('category'), {}, None),
        'runtime': (pd.to_numeric, {'errors': 'coerce'}, None),
        'spoken_languages': (lambda x: x.astype('category'), {}, None),
        'status': (lambda x: x.astype('category'), {}, None),
        'tagline': (lambda x: x.astype('string'), {}, None),
        'title': (lambda x: x.astype('string'), {}, None),
        'cast': (lambda x: x.astype('string'), {}, None),
        'director': (lambda x: x.astype('string'), {}, None),
        'cast_size': (pd.to_numeric, {'errors': 'coerce'}, None),
        'crew_size': (pd.to_numeric, {'errors': 'coerce'}, None),
        'vote_average': (pd.to_numeric, {'errors': 'coerce'}, None),
        'vote_count': (pd.to_numeric, {'errors': 'coerce'}, None)
    }
    for col, (func, kwargs, post_process) in conversions.items():
        if col in df.columns:
            df[col] = func(df[col], **kwargs)
            if post_process:
                df[col] = post_process(df[col])
        else:
            print(f"Warning: Column {col} not found for datatype conversion.")
    return df


def handle_unrealistic_values(df):
    """Replace unrealistic or invalid values with NaN."""
    df = df.copy()
    for col in ['budget', 'revenue', 'runtime']:
        if col in df.columns:
            df[col] = df[col].replace(0, np.nan)
    if 'vote_count' in df.columns and 'vote_average' in df.columns:
        df.loc[df['vote_count'] == 0, 'vote_average'] = np.nan
    for col in ['overview', 'tagline', 'title', 'original_language', 'status']:
        if col in df.columns:
            df[col] = df[col].replace(['', 'No Data'], np.nan)
    return df


def remove_duplicates_and_invalid_rows(df):
    """Remove duplicates and rows with invalid titles or IDs."""
    df = df.copy()
    num_duplicates = df.duplicated().sum()
    print(f"Number of duplicate rows: {num_duplicates}")
    df.drop_duplicates(inplace=True)

    if 'title' in df.columns and 'id' in df.columns:
        unknown_titles = df[df['title'].str.strip().str.lower() == 'unknown']
        missing_id_or_title = df[df['id'].isna() | df['title'].isna()]
        print(f"Rows with title='unknown': {len(unknown_titles)}")
        print(f"Rows with missing id or title: {len(missing_id_or_title)}")

        df = df.dropna(subset=['id', 'title'])
        df = df[df['title'].str.strip().str.lower() != 'unknown']
        print(f"DataFrame shape after dropping unknown titles: {df.shape}")
    else:
        print("Warning: 'title' or 'id' column missing. Skipping title/ID checks.")

    df = df.dropna(thresh=10)
    print(f"Cleaned DataFrame shape after dropping rows with < 10 non-null values: {df.shape}")
    return df


def filter_released_movies(df):
    """Filter for 'Released' movies and drop the 'status' column."""
    df = df.copy()
    if 'status' in df.columns:
        df = df[df['status'] == 'Released']
        df = df.drop(columns='status', inplace=False)
        print(f"Cleaned DataFrame shape after filtering for 'Released' movies: {df.shape}")
    else:
        print("Warning: 'status' column missing. Skipping released movies filter.")
    return df


def finalize_dataframe(df):
    """Rename columns, select final columns, and reset index."""
    df = df.copy()
    df.rename(columns={'budget': 'budget_musd', 'revenue': 'revenue_musd'}, inplace=True)
    final_columns = [
        'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
        'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
        'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
        'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
    ]
    available_columns = [col for col in final_columns if col in df.columns]
    if len(available_columns) < len(final_columns):
        print(f"Warning: Missing columns {set(final_columns) - set(available_columns)} in final selection.")
    df = df[available_columns]
    df.reset_index(drop=True, inplace=True)
    print(f"Cleaned DataFrame shape: {df.shape}")
    return df


def save_dataframe(df, save_path=None):
    """Save the DataFrame to a parquet file with a timestamp and record the timestamp in latest_timestamp.txt.
    
    Args:
        df (pd.DataFrame): DataFrame to save.
        save_path (str, optional): Path for the output Parquet file (e.g., 'cleaned_movies.parquet').
    """
    if save_path:
        try:
            # Generate timestamp in format YYYYMMDD_HHMMSS
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Convert save_path to Path object
            save_path = Path(save_path)
            
            # Use PROCESSED_DATA_DIR from config
            output_dir = Path(PROCESSED_DATA_DIR)
            
            # Create filename with timestamp
            name, ext = save_path.stem, save_path.suffix
            timestamped_filename = f"{name}_{timestamp}{ext}"
            timestamped_path = output_dir / timestamped_filename
            
            # Create output directory if it doesn't exist
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save DataFrame as Parquet
            df.to_parquet(timestamped_path, engine='pyarrow')
            print(f"Cleaned data saved to {timestamped_path}")
            
            # Save timestamp to latest_timestamp.txt
            timestamp_file = output_dir / "latest_timestamp.txt"
            with open(timestamp_file, 'w') as f:
                f.write(timestamp)
            print(f"Timestamp saved to {timestamp_file}")
            
        except Exception as e:
            print(f"Error saving DataFrame or timestamp: {e}")

def clean_data(df, save_path=None, inspect_columns=False, columns_to_inspect=None):
    """Clean and preprocess the movie DataFrame."""
    if df.empty:
        raise ValueError("Input DataFrame is empty.")
    
    df = drop_irrelevant_columns(df)
    df = evaluate_json_columns(df)
    if inspect_columns:
        inspect_extracted_columns(df, columns_to_inspect)
    df = convert_datatypes(df)
    df = handle_unrealistic_values(df)
    df = remove_duplicates_and_invalid_rows(df)
    df = filter_released_movies(df)
    df = finalize_dataframe(df)
    save_dataframe(df, save_path)
    return df