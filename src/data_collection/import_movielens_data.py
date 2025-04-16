import sqlite3
import csv
import pandas as pd
from config.settings import DB_PATH, RAW_DATA_PATH


# Load CSVs
ratings = pd.read_csv(RAW_DATA_PATH / "ratings.csv")
tags = pd.read_csv(RAW_DATA_PATH / "tags.csv")
links = pd.read_csv(RAW_DATA_PATH / "links.csv")

# Merge with links to get tmdb_id
ratings = ratings.merge(links[['movieId', 'tmdbId']], on='movieId', how='inner')
ratings = ratings.dropna(subset=['tmdbId'])
ratings['tmdbId'] = ratings['tmdbId'].astype(int)
ratings = ratings.rename(columns={'tmdbId': 'movie_id', 'userId': 'user_id'})
ratings = ratings.drop(columns='movieId')
ratings = ratings.drop_duplicates(subset=["user_id", "movie_id", "timestamp"])

tags = tags.merge(links[['movieId', 'tmdbId']], on='movieId', how='inner')
tags = tags.dropna(subset=['tmdbId'])
tags['tmdbId'] = tags['tmdbId'].astype(int)
tags = tags.rename(columns={'tmdbId': 'movie_id', 'userId': 'user_id'})
tags = tags.drop(columns='movieId')

# Create unique tag IDs
tags['tag_id'] = range(1, len(tags) + 1)


# Insert data
with sqlite3.connect(DB_PATH) as conn:
	ratings[['user_id', 'movie_id', 'rating', 'timestamp']].to_sql(
	    "user_movie_rating", conn, if_exists="append", index=False
	)

	tags[['tag_id', 'user_id', 'movie_id', 'tag', 'timestamp']].to_sql(
	    "user_movie_tag", conn, if_exists="append", index=False
	)

	links.rename(columns={"movieId": "movielens_id", "tmdbId": "tmdb_id", "imdbId": "imdb_id"}).to_sql(
	    "movie_link", conn, if_exists="replace", index=False
	)
