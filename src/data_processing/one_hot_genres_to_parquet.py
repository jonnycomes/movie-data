import pandas as pd
import sqlite3
from config.settings import DB_PATH, PROCESSED_DATA_PATH

with sqlite3.connect(DB_PATH) as conn:

    # Get distinct genre names
    genre_names = pd.read_sql_query("SELECT DISTINCT name FROM genre", conn)["name"].tolist()

    # Build SQL to one-hot encode genres per movie
    genre_columns = [
        f"MAX(CASE WHEN g.name = '{genre}' THEN 1 ELSE 0 END) AS genre_{genre.replace(' ', '_')}"
        for genre in genre_names
    ]
    genre_sql = ",\n    ".join(genre_columns)

    query = f"""
    SELECT
        m.*,
        {genre_sql}
    FROM movie AS m
    LEFT JOIN movie_genre AS mg ON m.movie_id = mg.movie_id
    LEFT JOIN genre AS g ON mg.genre_id = g.genre_id
    GROUP BY m.movie_id
    """

    df = pd.read_sql_query(query, conn)

# Output to a parquet file
output_path = PROCESSED_DATA_PATH / "movie_genres_onehot.parquet"
output_path.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(output_path, index=False)
print(f"Saved one-hot genre table to {output_path}")
