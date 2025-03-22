import sqlite3
import csv
from config.settings import DB_PATH, RAW_DATA_PATH

def import_links():
	with sqlite3.connect(DB_PATH) as conn:
		cursor = conn.cursor()

		# Read csv into a list:
		with open(RAW_DATA_PATH / 'links.csv', 'r') as f:
		    reader = csv.reader(f)
		    next(reader)  # Skip header row
		    data = list(reader)

	    # Some rows are missing tmdb link:
		partial_data = [row[:-1] for row in data if not row[-1]]
		full_data = [row for row in data if row[-1]]

		# Insert data without tmdb links into movies.db
		cursor.executemany("""
			INSERT INTO movie_link (movielens_id, imdb_id) 
			VALUES (?, ?)
			""", partial_data)

		# Insert data with tmdb links into movies.db
		cursor.executemany("""
			INSERT INTO movie_link (movielens_id, imdb_id, tmdb_id) 
			VALUES (?, ?, ?)
			""", full_data)

def import_ratings():
	with sqlite3.connect(DB_PATH) as conn:
		cursor = conn.cursor()

		# Read csv and insert into the db:
		with open(RAW_DATA_PATH / 'ratings.csv', 'r') as f:
		    reader = csv.reader(f)
		    next(reader)  # Skip header row
		    cursor.executemany("""
				INSERT INTO user_movie_rating (user_id, movielens_id, rating, timestamp) 
				VALUES (?, ?, ?, ?)
				""", reader)


if __name__ == '__main__':
	import_links()
	import_ratings()
