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

# Get the max timestamp and trim the DataFrame to avoid partial windows
max_date = rating_df["timestamp"].max()
cutoff_date = max_date - pd.Timedelta(weeks=9)
rating_df = rating_df[rating_df["timestamp"] <= cutoff_date]

# Count number of ratings per day
ratings_per_day = (
    rating_df
    .set_index("timestamp")
    .resample("D")
    .size()
    .rename("daily_count")
    .to_frame()
)

# Compute forward-looking rolling sums for 1 to 9 weeks (7, 14, 21, 28, 35,... days)
for weeks in range(1, 10):
    days = weeks * 7
    col_name = f"forward_{weeks}w_volume"
    ratings_per_day[col_name] = (
        ratings_per_day[::-1]  # Reverse for forward-looking
        .rolling(f"{days}D")
        .sum()
        .iloc[::-1]["daily_count"]
    )

# Reset index for output
ratings_per_day = ratings_per_day.reset_index()

# Drop the original daily count column
ratings_per_day = ratings_per_day.drop(columns="daily_count")

# Save to Parquet
output_path = PROCESSED_DATA_PATH / "daily_forward_multiweek_rating_volume.parquet"
output_path.parent.mkdir(parents=True, exist_ok=True)
ratings_per_day.to_parquet(output_path, index=False)
print(f"Saved daily multi-week forward rating volumes to {output_path}")
