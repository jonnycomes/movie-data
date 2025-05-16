CREATE TABLE IF NOT EXISTS movie (
    movie_id INTEGER PRIMARY KEY,
    title TEXT,
    release_date TEXT,
    budget INTEGER,
    revenue INTEGER,
    runtime INTEGER,
    vote_average REAL,
    vote_count INTEGER,
    popularity REAL
);
  
CREATE TABLE IF NOT EXISTS genre (
    genre_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genre (
    movie_id INTEGER,
    genre_id INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS keyword (
    keyword_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_keyword (
    movie_id INTEGER,
    keyword_id INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (keyword_id) REFERENCES keyword(keyword_id),
    PRIMARY KEY (movie_id, keyword_id)
);

CREATE TABLE IF NOT EXISTS person (
    person_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_cast (
    movie_id INTEGER,
    person_id INTEGER,
    character TEXT,
    cast_order INTEGER,
    PRIMARY KEY (movie_id, person_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (person_id) REFERENCES person(person_id)
);

CREATE TABLE IF NOT EXISTS movie_crew (
    movie_id INTEGER,
    person_id INTEGER,
    job TEXT,
    department TEXT,
    PRIMARY KEY (movie_id, person_id, job),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (person_id) REFERENCES person(person_id)
);

CREATE TABLE IF NOT EXISTS production_company (
    company_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_production_company (
    movie_id INTEGER,
    company_id INTEGER,
    PRIMARY KEY (movie_id, company_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (company_id) REFERENCES production_company(company_id)
);

CREATE TABLE IF NOT EXISTS user_movie_rating (
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating REAL NOT NULL,
    timestamp INTEGER,
    PRIMARY KEY (user_id, movie_id, timestamp)
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id)   
);

CREATE TABLE IF NOT EXISTS user_movie_tag (
    tag_id INTEGER,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    tag TEXT,
    timestamp INTEGER,
    PRIMARY KEY (tag_id)
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id)   
);

CREATE TABLE IF NOT EXISTS movie_link (
    movielens_id INTEGER,
    tmdb_id INTEGER,
    imdb_id INTEGER,
    PRIMARY KEY (movielens_id),
    FOREIGN KEY (tmdb_id) REFERENCES movie(movie_id)
);
