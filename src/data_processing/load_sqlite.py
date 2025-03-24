import sqlite3
import pandas as pd
from config.settings import DB_PATH, DATA_PROCESSING_SQL_PATH as SQL_PATH


def fetch_one_hot_genres(vote_count_min=0):
    """
    Fetches a one-hot encoded DataFrame for movie genres from an SQLite database.

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
    
    Example:
    --------
    >>> df = fetch_one_hot_genres()
    >>> print(df.head())

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
        case_statements = [f"MAX(CASE WHEN gp.genre_name = ? THEN 1 ELSE 0 END) AS `{genre}`" for genre in genres]

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

def fetch_scores_by_tmdb(lambda_director, lambda_writers, lambda_cast_time, lambda_cast_order):
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
                               lambda_cast_time, lambda_cast_order
                               ))
        
        # Fetch and return the results into a pandas DataFrame
        return pd.DataFrame(cursor.fetchall(), columns=["movie_id", "director_score", "writer_score", "cast_score", "production_company_score"])

def fetch_predict_success_data(lambda_director, lambda_writers, lambda_cast_time, lambda_cast_order):
    threshold = 7.0

    # Get initial numeric features
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        query = """
            SELECT movie_id, vote_average, runtime, num_cast_members, release_date
            FROM movie_rating_features;
            """
        df = pd.read_sql_query(query, conn)

    # Convert release_date to numeric
    df['release_date'] = pd.to_datetime(df['release_date'])
    df['release_date'] = df['release_date'].astype(int) / 10**9  # Unix timestamp in seconds

    # Add genres
    df_genre = fetch_one_hot_genres(vote_count_min=30)
    df = pd.merge(df, df_genre, on='movie_id')

    # Add scores (only using tmdb data)
    df_scores = fetch_scores_by_tmdb(lambda_director, lambda_writers, lambda_cast_time, lambda_cast_order)
    df = pd.merge(df, df_scores, on='movie_id')

    df["successful"] = (df["vote_average"] > threshold).astype(int) 

    return df

def fetch_movie_rating_features():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        query = """SELECT * FROM movie_rating_features;"""
        return pd.read_sql_query(query, conn)


if __name__ == '__main__':
    fetch_scores(1,1,1,1)