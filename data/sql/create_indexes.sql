CREATE INDEX IF NOT EXISTS idx_movie_id ON movie(movie_id);
CREATE INDEX IF NOT EXISTS idx_person_id ON person(person_id);
CREATE INDEX IF NOT EXISTS idx_movielens_id ON movie_link(movielens_id);
CREATE INDEX IF NOT EXISTS idx_movie_link_tmdb_id ON movie_link(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_user_movie_rating_movielens_id ON user_movie_rating(movielens_id);
CREATE INDEX IF NOT EXISTS idx_user_movie_rating_rating ON user_movie_rating(rating);

