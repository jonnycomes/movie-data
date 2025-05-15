import pandas as pd
import sqlite3
from config.settings import DB_PATH, PROCESSED_DATA_PATH

# Define start date and convert to Unix timestamp for filtering
start_date = pd.Timestamp("2016-01-01") 
start_timestamp = int(start_date.timestamp())

# Load and filter ratings directly in SQL
with sqlite3.connect(DB_PATH) as conn:
    rating_df = pd.read_sql_query(
        f"""
        SELECT user_id, movie_id, rating, timestamp
        FROM user_movie_rating
        WHERE timestamp >= {start_timestamp}
        """,
        conn
    )

# Convert timestamp to datetime
rating_df["timestamp"] = pd.to_datetime(rating_df["timestamp"], unit="s")

# Count number of ratings per day
ratings_per_day = (
    rating_df
    .set_index("timestamp")
    .resample("D")
    .size()
    .rename("daily_count")
    .to_frame()
)

# Compute forward-looking 4-week (28-day) rolling sum starting from each day
ratings_per_day["forward_4w_volume"] = (
    ratings_per_day[::-1]  # reverse to use backward-looking window as forward
    .rolling("28D")
    .sum()
    .iloc[::-1]["daily_count"]
)

# Trim to exclude rows where 4-week window would exceed last date
max_date = ratings_per_day.index.max()
cutoff_date = max_date - pd.Timedelta(days=28)
ratings_per_day = ratings_per_day.loc[ratings_per_day.index <= cutoff_date]

# Reset index for output
ratings_per_day = ratings_per_day.reset_index()

# Save to Parquet
output_path = PROCESSED_DATA_PATH / "daily_forward_4w_rating_volume.parquet"
output_path.parent.mkdir(parents=True, exist_ok=True)
ratings_per_day.to_parquet(output_path, index=False)
print(f"Saved daily forward 4-week rating volume to {output_path}")
