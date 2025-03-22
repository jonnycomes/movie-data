import sqlite3
import requests
import calendar
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import DB_PATH, TMDB_API_KEY, TMDB_BASE_URL, TMDB_REQUEST_PAGE_LIMIT


def fetch_movies(start_date, end_date, page, min_votes):
    """
    Fetches a list of movies released between start_date and end_date from TMDb.

    Args:
        start_date (str): The start date in "YYYY-MM-DD" format.
        end_date (str): The end date in "YYYY-MM-DD" format.
        page (int): The page number to fetch.
        min_votes (int): Only movies with at least min_votes votes will be fetched.

    Returns:
        dict: JSON response containing movie data, or None if the request fails.
    """
    params = {
        "api_key": TMDB_API_KEY,
        "vote_count.gte": min_votes,
        "primary_release_date.gte": start_date,
        "primary_release_date.lte": end_date,
        "page": page,
        "sort_by": "revenue.desc"
    }
    response = requests.get(f"{TMDB_BASE_URL}/discover/movie", params=params)
    return response.json() if response.status_code == 200 else None


def fetch_movie_details(movie_id):
    """
    Fetches detailed movie information.

    Args:
        movie_id (int): The TMDb movie ID.

    Returns:
        dict: JSON response with movie details, or None if the request fails.
    """
    params = {"api_key": TMDB_API_KEY}
    url = f"{TMDB_BASE_URL}/movie/{movie_id}"

    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


def split_date_range(start_date, end_date, min_votes):
    """
    Dynamically splits a date range into smaller chunks if needed to stay within TMDb's 500-page limit.

    Args:
        start_date (str): The starting date in "YYYY-MM-DD" format.
        end_date (str): The ending date in "YYYY-MM-DD" format.
        min_votes (int): Only movies with at least min_votes votes will be checked.

    Returns:
        list: A list of (start_date, end_date) tuples representing smaller time chunks.
    """
    date_ranges = [(start_date, end_date)]
    while date_ranges:
        start, end = date_ranges.pop(0)
        data = fetch_movies(start, end, 1, min_votes)  # Fetch page 1 to check total pages
        total_pages = data.get("total_pages", 1) if data else 1

        if total_pages > TMDB_REQUEST_PAGE_LIMIT:
            midpoint = datetime.strptime(start, "%Y-%m-%d") + (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")) / 2
            mid_date = midpoint.strftime("%Y-%m-%d")

            # Split into two halves
            date_ranges.append((start, mid_date))
            date_ranges.append((mid_date, end))
        else:
            yield (start, end)  # Only return valid, small-enough ranges


def process_movies_parallel(start_date, end_date, min_votes):
    """
    Fetches movie data within a given date range and retrieves detailed movie information in parallel.

    Args:
        start_date (str): The start date in "YYYY-MM-DD" format.
        end_date (str): The end date in "YYYY-MM-DD" format.
        min_votes (int): Only movies with at least min_votes votes will be processed.

    Returns:
        list: A list of tuples (movie_summary, movie_details).
    """
    movies_to_fetch = []

    for sub_start, sub_end in split_date_range(start_date, end_date, min_votes):
        page = 1
        total_pages = None

        while total_pages is None or page <= total_pages:
            data = fetch_movies(sub_start, sub_end, page, min_votes)
            if not data:
                break

            if page == 1:
                total_pages = data.get("total_pages", 1)
                print(f"Fetching {total_pages} pages from {sub_start} to {sub_end}...")

            movies_to_fetch.extend(data.get("results", []))
            page += 1

    # Fetch movie details in parallel
    movie_details_list = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_movie = {executor.submit(fetch_movie_details, movie["id"]): movie for movie in movies_to_fetch}
        for future in as_completed(future_to_movie):
            movie = future_to_movie[future]
            movie_details = future.result()
            if movie_details:
                movie_details_list.append((movie, movie_details))

    return movie_details_list


def save_movies_parallel(start_year, end_year, min_votes):
    """
    Orchestrates fetching and storing movie data in parallel.

    Fetches movies from TMDb, retrieves details in parallel, and inserts data into SQLite DB.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for year in range(start_year, end_year + 1):
        with ThreadPoolExecutor(max_workers=5) as executor:  # Parallelize across months
            future_to_month = {
                executor.submit(process_movies_parallel, f"{year}-{month:02d}-01", f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}", min_votes)
                for month in range(1, 13)
            }

            for future in as_completed(future_to_month):
                movie_details_list = future.result()

                # Prepare batch insert data
                movie_rows = []
                genre_rows = set()
                movie_genre_rows = []
                keyword_rows = set()
                movie_keyword_rows = []
                person_rows = set()
                movie_cast_rows = []
                movie_crew_rows = []
                company_rows = set()
                movie_production_rows = []

                for movie, details in movie_details_list:
                    movie_rows.append((
                        movie["id"], movie["title"], movie.get("release_date"),
                        details.get("budget"), details.get("revenue"),
                        details.get("runtime"), movie.get("vote_average"),
                        movie.get("vote_count"), movie.get("popularity")
                    ))

                    for genre in details.get("genres", []):
                        genre_rows.add((genre["id"], genre["name"]))
                        movie_genre_rows.append((movie["id"], genre["id"]))

                    for keyword in details.get("keywords", {}).get("keywords", []):
                        keyword_rows.add((keyword["id"], keyword["name"]))
                        movie_keyword_rows.append((movie["id"], keyword["id"]))

                    for person in details.get("credits", {}).get("cast", []) + details.get("credits", {}).get("crew", []):
                        person_rows.add((person["id"], person["name"]))
                        if "cast_id" in person:
                            movie_cast_rows.append((movie["id"], person["id"], person.get("character"), person.get("order")))
                        else:
                            movie_crew_rows.append((movie["id"], person["id"], person.get("job"), person.get("department")))

                    for company in details.get("production_companies", []):
                        company_rows.add((company["id"], company["name"]))
                        movie_production_rows.append((movie["id"], company["id"]))

                # Perform batch inserts
                cursor.executemany("INSERT OR IGNORE INTO movie (movie_id, title, release_date, budget, revenue, runtime, vote_average, vote_count, popularity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", movie_rows)
                cursor.executemany("INSERT OR IGNORE INTO genre (genre_id, name) VALUES (?, ?)", list(genre_rows))
                cursor.executemany("INSERT OR IGNORE INTO movie_genre (movie_id, genre_id) VALUES (?, ?)", movie_genre_rows)
                cursor.executemany("INSERT OR IGNORE INTO keyword (keyword_id, name) VALUES (?, ?)", list(keyword_rows))
                cursor.executemany("INSERT OR IGNORE INTO movie_keyword (movie_id, keyword_id) VALUES (?, ?)", movie_keyword_rows)
                cursor.executemany("INSERT OR IGNORE INTO person (person_id, name) VALUES (?, ?)", list(person_rows))
                cursor.executemany("INSERT OR IGNORE INTO movie_cast (movie_id, person_id, character, cast_order) VALUES (?, ?, ?, ?)", movie_cast_rows)
                cursor.executemany("INSERT OR IGNORE INTO movie_crew (movie_id, person_id, job, department) VALUES (?, ?, ?, ?)", movie_crew_rows)
                cursor.executemany("INSERT OR IGNORE INTO production_company (company_id, name) VALUES (?, ?)", list(company_rows))
                cursor.executemany("INSERT OR IGNORE INTO movie_production_company (movie_id, company_id) VALUES (?, ?)", movie_production_rows)

                conn.commit() 

    conn.close()
    print("Movie data insertion complete.")



def save_movie(tmdb_id):
    """Fetches movie details from TMDb and saves all relevant data to the database."""
    
    # Fetch data from TMDb API
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}?api_key={TMDB_API_KEY}&append_to_response=credits"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Error fetching data for TMDB ID {tmdb_id}")
        return
    
    data = response.json()

    # Extract movie details
    movie_id = data["id"]
    title = data.get("title")
    release_date = data.get("release_date")
    budget = data.get("budget", 0)
    revenue = data.get("revenue", 0)
    runtime = data.get("runtime")
    vote_average = data.get("vote_average", 0.0)
    vote_count = data.get("vote_count", 0)
    popularity = data.get("popularity", 0.0)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # Insert into movie table
        cursor.execute("""
            INSERT INTO movie (movie_id, title, release_date, budget, revenue, runtime, vote_average, vote_count, popularity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(movie_id) DO NOTHING;
        """, (movie_id, title, release_date, budget, revenue, runtime, vote_average, vote_count, popularity))

        # Insert genres into genre and movie_genre tables
        for genre in data.get("genres", []):
            genre_id = genre["id"]
            genre_name = genre["name"]

            cursor.execute("""
                INSERT INTO genre (genre_id, name)
                VALUES (?, ?)
                ON CONFLICT(genre_id) DO NOTHING;
            """, (genre_id, genre_name))

            cursor.execute("""
                INSERT INTO movie_genre (movie_id, genre_id)
                VALUES (?, ?)
                ON CONFLICT(movie_id, genre_id) DO NOTHING;
            """, (movie_id, genre_id))

        # Insert production companies
        for company in data.get("production_companies", []):
            company_id = company["id"]
            company_name = company["name"]

            cursor.execute("""
                INSERT INTO production_company (company_id, name)
                VALUES (?, ?)
                ON CONFLICT(company_id) DO NOTHING;
            """, (company_id, company_name))

            cursor.execute("""
                INSERT INTO movie_production_company (movie_id, company_id)
                VALUES (?, ?)
                ON CONFLICT(movie_id, company_id) DO NOTHING;
            """, (movie_id, company_id))

        # Insert cast members
        credits = data.get("credits", {})
        for cast in credits.get("cast", []):
            person_id = cast["id"]
            name = cast["name"]
            character = cast.get("character", "")
            cast_order = cast["order"]

            cursor.execute("""
                INSERT INTO person (person_id, name)
                VALUES (?, ?)
                ON CONFLICT(person_id) DO NOTHING;
            """, (person_id, name))

            cursor.execute("""
                INSERT INTO movie_cast (movie_id, person_id, character, cast_order)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(movie_id, person_id) DO NOTHING;
            """, (movie_id, person_id, character, cast_order))

        # Insert crew members
        for crew in credits.get("crew", []):
            person_id = crew["id"]
            name = crew["name"]
            job = crew["job"]
            department = crew["department"]

            cursor.execute("""
                INSERT INTO person (person_id, name)
                VALUES (?, ?)
                ON CONFLICT(person_id) DO NOTHING;
            """, (person_id, name))

            cursor.execute("""
                INSERT INTO movie_crew (movie_id, person_id, job, department)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(movie_id, person_id, job) DO NOTHING;
            """, (movie_id, person_id, job, department))



if __name__ == "__main__":
    session = requests.Session()

    start_year = 2003  
    end_year = 2025
    min_votes = 0    

    save_movies_parallel(start_year, end_year, min_votes)

    # print(fetch_movie_details(139405))
