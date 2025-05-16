import os
import subprocess
import runpy
import urllib.request
import zipfile
from io import BytesIO
from pathlib import Path


# Prompt user for TMDB API key and save to secret_settings.py
TMDB_KEY_PATH = Path("src/config/secret_settings.py")
if not TMDB_KEY_PATH.exists():
    print("Please enter your TMDB API key:")
    tmdb_api_key = input("> ").strip()
    TMDB_KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TMDB_KEY_PATH, "w") as f:
        f.write(f'TMDB_API_KEY = "{tmdb_api_key}"\n')
    print("TMDB API key saved to src/config/secret_settings.py.")
else:
    print("TMDB API key file already exists. Skipping creation.")

# Create SQLite database schema
print("Creating database schema...")
subprocess.run(["sqlite3", "data/movies.db", ".read data/sql/create_tables.sql"])
subprocess.run(["sqlite3", "data/movies.db", ".read data/sql/create_indexes.sql"])
subprocess.run(["sqlite3", "data/movies.db", ".read data/sql/create_views.sql"])

# Download and import MovieLens data
print("Downloading MovieLens data...")
raw_dir = Path("data/raw")
raw_dir.mkdir(parents=True, exist_ok=True)

movielens_url = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
response = urllib.request.urlopen(movielens_url)

with zipfile.ZipFile(BytesIO(response.read())) as zip_file:
    for name in ["links.csv", "movies.csv", "ratings.csv", "tags.csv"]:
        zip_file.extract(f"ml-latest-small/{name}", path=raw_dir)
        (raw_dir / "ml-latest-small" / name).rename(raw_dir / name)
    (raw_dir / "ml-latest-small").rmdir()

print("Importing MovieLens data...")
runpy.run_module("src.data_collection.import_movielens_data", run_name="__main__")

# Import TMDb metadata
print("Importing TMDb data...")
runpy.run_module("src.data_collection.fetch_tmdb_movies", run_name="__main__")

# Generate processed data files
Path("data/processed").mkdir(parents=True, exist_ok=True)
print("Generating processed data...")
runpy.run_module("src.data_processing.daily_forward_4w_rating_volume_to_parquet", run_name="__main__")
runpy.run_module("src.data_processing.daily_forward_multiweek_rating_volume_to_parquet", run_name="__main__")
runpy.run_module("src.data_processing.one_hot_genres_to_parquet", run_name="__main__")

print("Data setup complete.")
