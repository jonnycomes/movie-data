import sqlite3
import pandas as pd
from config.settings import DB_PATH, DATA_PROCESSING_SQL_PATH as SQL_PATH


def fetch_one_hot_genres(vote_count_min=0):
    """
    Fetches a one-hot encoded DataFrame for movie genres.

    Each row represents a movie, with columns indicating whether a movie belongs to a particular genre (1 for yes, 0 for no).  
    The function dynamically adjusts to all genres present in the database.

    Parameters:
    -----------
    vote_count_min : numeric, optional
        The minimum vote count a movie must have to be included in the results. Default is 0.

    Returns:
    --------
    pd.DataFrame
        A DataFrame where:
        - The first column is `movie_id`.
        - Subsequent columns are genre names, each containing 1 (if the movie has that genre) or 0 (otherwise).

    Notes:
    ------
    - Only genres listed in the `genre` table are considered.
    - If a movie has no listed genres, all genre columns will be 0.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # Get all unique genres from the genre table
        cursor.execute("SELECT DISTINCT name FROM genre")
        genres = [row[0] for row in cursor.fetchall()]

        # Return an empty DataFrame if no genres exist
        if not genres:
            return pd.DataFrame(columns=["movie_id"])

        # Dynamically generate the SQL query
        case_statements = [f"MAX(CASE WHEN gp.genre_name = ? THEN 1 ELSE 0 END) AS `genre_{genre}`" for genre in genres]

        query = f"""
        WITH GenrePivot AS (
            SELECT 
                mg.movie_id, 
                g.name AS genre_name
            FROM movie_genre mg
            JOIN genre g ON mg.genre_id = g.genre_id
        )
        SELECT 
            m.movie_id,
            {", ".join(case_statements)}
        FROM movie m
        LEFT JOIN GenrePivot gp ON m.movie_id = gp.movie_id
        WHERE m.vote_count >={vote_count_min}
        GROUP BY m.movie_id;
        """

        # Execute the query with parameterized genre values
        return pd.read_sql_query(query, conn, params=genres)



def fetch_scores(lambda_director, lambda_writers, lambda_cast_time, lambda_cast_order):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
    
        # Read the SQL query from the file generate_scores.sql
        with open(SQL_PATH / 'generate_scores.sql', 'r') as f:
            query = f.read()
        
        # Execute the query with the passed parameters
        cursor.execute(query, (lambda_director, lambda_director, 
                               lambda_writers, lambda_writers, 
                               lambda_cast_time, lambda_cast_order,
                               lambda_cast_time, lambda_cast_order,
                               lambda_cast_time, lambda_cast_order
                               ))
        
        # Fetch and return the results into a pandas DataFrame
        return pd.DataFrame(cursor.fetchall(), columns=["movie_id", "director_score", "writer_score", "cast_score", "production_company_score"])

def fetch_scores_by_tmdb(lambda_director, lambda_writers, lambda_cast_time, lambda_cast_order, min_votes=30):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
    
        # Read the SQL query from the file generate_scores.sql
        with open(SQL_PATH / 'generate_scores_by_tmdb.sql', 'r') as f:
            query = f.read()
        
        # Execute the query with the passed parameters
        cursor.execute(query, (lambda_director, lambda_director, 
                               lambda_writers, lambda_writers, 
                               lambda_cast_time, lambda_cast_order,
                               lambda_cast_time, lambda_cast_order,
                               lambda_cast_time, lambda_cast_order,
                               min_votes
                               ))
        
        # Fetch and return the results into a pandas DataFrame
        return pd.DataFrame(cursor.fetchall(), columns=["movie_id", "director_score", "writer_score", "cast_score", "production_company_score"])

def fetch_predict_success_data(lambda_director, lambda_writers, lambda_cast_time, lambda_cast_order, min_votes=30):
    # Get initial numeric features
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        query = """
            SELECT *
            -- movie_id, vote_average, runtime, num_cast_members, release_date
            FROM movie_rating_features;
            """
        df = pd.read_sql_query(query, conn)

    # Add genres
    df_genre = fetch_one_hot_genres(vote_count_min=min_votes)
    df = pd.merge(df, df_genre, on='movie_id')

    # Add scores (only using tmdb data)
    df_scores = fetch_scores_by_tmdb(lambda_director, lambda_writers, lambda_cast_time, lambda_cast_order, min_votes)
    df = pd.merge(df, df_scores, on='movie_id')

    return df

def fetch_movie_rating_features(min_votes=30):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        query = f"""
                SELECT * FROM movie_rating_features
                WHERE vote_count >= {min_votes}
                ;"""
        return pd.read_sql_query(query, conn)

def fetch_movies(add_one_hot_genres=False):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        query = """SELECT * FROM movie;"""
        df = pd.read_sql_query(query, conn)

    df['release_date'] = pd.to_datetime(df['release_date'])
    if add_one_hot_genres:
        df = pd.merge(df, fetch_one_hot_genres(), on='movie_id')

    return df

def fetch_user_movie_ratings(tmdb_only=False):
    if tmdb_only:
        query = """
                SELECT ml.tmdb_id AS movie_id, umr.user_id, umr.rating, umr.timestamp
                FROM user_movie_rating umr 
                JOIN movie_link ml ON ml.movielens_id = umr.movielens_id
                WHERE ml.tmdb_id IS NOT NULL
                """
    else:
        query = "SELECT * FROM user_movie_rating"

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        return pd.read_sql_query(query, conn)


def fetch_tmdb_to_movielens_id_map():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        query = """
                SELECT tmdb_id, movielens_id 
                FROM movie_link
                WHERE tmdb_id IS NOT NULL
                """
        return pd.read_sql_query(query, conn)

def fetch_movie_title(movie_id, include_release_year=True):
    if include_release_year:
        query = f"""
                SELECT title || ' (' || strftime('%Y', release_date) ||')' 
                FROM movie WHERE movie_id={movie_id}
                """
    else:
        query = f"SELECT title FROM movie WHERE movie_id={movie_id}"

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]  

if __name__ == '__main__':
    print(fetch_movie_title(603, True))