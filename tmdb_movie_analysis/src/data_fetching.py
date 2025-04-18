import requests
import pandas as pd
import json
from datetime import datetime
import os
from src.config import TMDB_API_KEY, BASE_URL, MOVIE_IDS


def fetch_movie_data(movie_ids=MOVIE_IDS, save_path=None):
    """Fetch movie data from TMDb API for given movie IDs, skipping any that return errors."""
    movies = []
    failed_ids = []

    for movie_id in movie_ids:
        url = f"{BASE_URL}/{movie_id}?api_key={TMDB_API_KEY}&append_to_response=credits"

        try:
            response = requests.get(url)
            if response.status_code == 200:
                movie_data = response.json()
                movies.append(movie_data)
            else:
                print(f"❌ Movie ID {movie_id} failed with status {response.status_code}")
                failed_ids.append(movie_id)

        except requests.exceptions.RequestException as e:
            print(f"❌ Network error for movie ID {movie_id}: {e}")
            failed_ids.append(movie_id)

        except json.JSONDecodeError:
            print(f"❌ Invalid JSON for movie ID {movie_id}")
            failed_ids.append(movie_id)

    df = pd.DataFrame(movies)

    # Ensure save directories exist
    os.makedirs("data/raw", exist_ok=True)

    # Generate save path with timestamp if not provided
    if not df.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = save_path or f"data/raw/movies_{timestamp}.json"

        # Save the movie data
        df.to_json(save_path, orient="records", lines=True)
        print(f"\n💾 Data saved to {save_path}")

        # Save the latest timestamp for use in cleaning step
        with open("data/latest_timestamp.txt", "w") as f:
            f.write(timestamp)
        print("🕒 Latest timestamp recorded.")

    else:
        print("\n⚠️ No data to save.")

    print(f"\n✅ Successfully fetched {len(df)} movies.")
    if failed_ids:
        print(f"❌ These movie IDs failed: {failed_ids}")

    return df


if __name__ == "__main__":
    df = fetch_movie_data()
    print("\n🔍 Sample of fetched data:")
    print(df.head())
