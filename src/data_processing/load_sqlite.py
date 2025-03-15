import sqlite3
import pandas as pd

def fetch_one_hot_genres(db_path="../data/movies.db", vote_count_min=0):
    """
    Fetches a one-hot encoded DataFrame for movie genres from an SQLite database.

    Each row represents a movie, with columns indicating whether a movie belongs to a particular genre (1 for yes, 0 for no).  
    The function dynamically adjusts to all genres present in the database.

    Parameters:
    -----------
    db_path : str, optional
        The file path to the SQLite database. Defaults to "movies.db".

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
    with sqlite3.connect(db_path) as conn:
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
