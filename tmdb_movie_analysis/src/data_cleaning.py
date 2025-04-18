import pandas as pd
import numpy as np
import os    

def clean_data(df, save_path=None):
    """Clean and preprocess the movie Dataframe."""
    # Drop irrelevant columns
    columns_to_drop = ['adult','backdrop_path', 'imdb_id', 'original_title', 'video', 'homepage']
    df = df.drop(columns=columns_to_drop, errors='ignore')

    # Evaluate JSON-like columns
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
    df["cast"] = df["credits"].apply(
        lambda x: ", ".join([person["name"] for person in x.get("cast", [])])
    )
    df["cast_size"] = df["credits"].apply(
        lambda x: len(x.get("cast", []))
    )
    df["director"] = df["credits"].apply(
    lambda x: ", ".join([person["name"] for person in x.get("crew", []) if person.get("job") == "Director"])
    )
    df["crew_size"] = df["credits"].apply(
    lambda x: len(x.get("crew", []))
    )
    
    """ Inspect extracted columns using value_counts """
    # Belongs to collection
    print("Collection name distribution:")
    print(df["belongs_to_collection"].value_counts(dropna=False))
    # Genres
    print("\nGenres distribution:")
    print(df["genres"].value_counts(dropna=False))
    # Spoken Languages
    print("\nSpoken languages distribution:")
    print(df["spoken_languages"].value_counts(dropna=False))
    # Production countries
    print("\nProduction countries distribution:")
    print(df["production_countries"].value_counts(dropna=False))
    # Cast
    print("\nCast distribution:")
    print(df["cast"].value_counts(dropna=False))
    # Cast size
    print("\nCast size distribution:")
    print(df["cast_size"].value_counts(dropna=False))
    # Director
    print("\nDirector distribution:")
    print(df["director"].value_counts(dropna=False))    
    # Crew size
    print("\nCrew size distribution:")
    print(df["crew_size"].value_counts(dropna=False))
    # Production companies
    print("\nProduction companies distribution:")
    print(df["production_companies"].value_counts(dropna=False))

    # Convert datatypes
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce') / 1000000
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce') / 1000000
    df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['belongs_to_collection'] = df['belongs_to_collection'].astype('category')
    df['genres'] = df['genres'].astype('category')
    df['id'] = df['id'].astype('int64')
    df['original_language'] = df['original_language'].astype('category')
    df['origin_country'] = df['origin_country'].astype('category')
    df['overview'] = df['overview'].astype('string')
    df['poster_path'] = df['poster_path'].astype('string')
    df['production_companies'] = df['production_companies'].astype('category')
    df['production_countries'] = df['production_countries'].astype('category')
    df['runtime'] = pd.to_numeric(df['runtime'], errors='coerce')
    df['spoken_languages'] = df['spoken_languages'].astype('category')
    df['status'] = df['status'].astype('category')
    df['tagline'] = df['tagline'].astype('string')
    df['title'] = df['title'].astype('string')
    df['cast'] = df['cast'].astype('string')
    df['director'] = df['director'].astype('string')
    df['cast_size'] = pd.to_numeric(df['cast_size'], errors='coerce')
    df['crew_size'] = pd.to_numeric(df['crew_size'], errors='coerce')
    df['vote_average'] = pd.to_numeric(df['vote_average'], errors='coerce')
    df['vote_count'] = pd.to_numeric(df['vote_count'], errors='coerce')



    # Handle unrealistic values
    df['budget'] = df['budget'].replace(0, np.nan)
    df['revenue'] = df['revenue'].replace(0, np.nan)
    df['runtime'] = df['runtime'].replace(0, np.nan)
    df.loc[df['vote_count']==0, 'vote_average'] = np.nan
    df['overview'] = df['overview'].replace(['','No Data'], np.nan)
    df['tagline'] =df['tagline'].replace(['','No Data'], np.nan)
    df['title'] = df['title'].replace(['','No Data'], np.nan)
    df['original_language'] = df['original_language'].replace(['','No Data'], np.nan)
    df['status'] = df['status'].replace(['','No Data'], np.nan) 


    # Drop credits column since we have extracted the relevant information
    df = df.drop(columns=['credits'], errors='ignore')

    # Checking and removing duplicates
    num_duplicates = df.duplicated().sum()
    print(f"Number of duplicate rows: {num_duplicates}")
    df.drop_duplicates(inplace=True)

    # Check for "unknown" titles or missing IDs/titles
    unknown_titles = df[df['title'].str.strip().str.lower() == 'unknown']
    missing_id_or_title = df[df['id'].isna() | df['title'].isna()]
    print(f"Rows with title='unknown': {len(unknown_titles)}")
    print(f"Rows with missing id or title: {len(missing_id_or_title)}")

    # Drop rows with unknown title or id
    df = df.dropna(subset=['id', 'title'])
    df = df[df['title'].str.strip().str.lower() != 'unknown']
    print(f"DataFrame shape after dropping unknown titles: {df.shape}")

    # Keep only rows with at least 10 non-null values
    df = df.dropna(thresh=10)
    print(f"Cleaned DataFrame shape after dropping rows with < 10 non-null values: {df.shape}")


    # Filter to include only 'Released' movies and drop the 'status' column
    df = df[df['status'] == 'Released']
    df.drop(columns='status', inplace=True)
    print(f"Cleaned DataFrame shape after filtering for 'Released' movies: {df.shape}")

    # Rename columns according to final dataframe structure
    df.rename(columns={
        'budget': 'budget_musd',
        'revenue': 'revenue_musd'
    }, inplace=True)

    final_columns = [
        'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
        'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
        'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
        'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
    ]

    df = df[final_columns]
    print(f"Cleaned DataFrame shape: {df.shape}")

    # Reset index
    df.reset_index(drop=True, inplace=True)

    # Save to parquet file if save_path is provided
    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_parquet(save_path, engine='pyarrow')
        print(f"Cleaned data saved to {save_path}")
    return df

if __name__ == "__main__":
    with open(f"data/latest_timestamp.txt", "r") as f:
        latest_timestamp = f.read().strip()
    raw_df = pd.read_json(f"data/raw/movies_{latest_timestamp}.json", lines=True)

    save_path = f"data/processed/cleaned_movies_{latest_timestamp}.parquet"
    cleaned_df = clean_data(raw_df, save_path)
    print(cleaned_df.head())



    
