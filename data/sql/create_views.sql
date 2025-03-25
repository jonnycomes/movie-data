DROP VIEW IF EXISTS movie_rating_features;
DROP VIEW IF EXISTS movie_scores;

CREATE VIEW movie_rating_features AS
    SELECT 
        m.movie_id,
        m.vote_average,
        m.title,
        m.release_date,
        m.runtime,

        -- top cast member by billing order
        (SELECT GROUP_CONCAT(person_id) 
         FROM (SELECT mc.person_id 
               FROM movie_cast AS mc 
               WHERE mc.movie_id = m.movie_id 
               ORDER BY mc.cast_order ASC 
               LIMIT 1)) AS top_cast_id,

        -- top 2 cast members by billing order
        (SELECT GROUP_CONCAT(person_id) 
         FROM (SELECT mc.person_id 
               FROM movie_cast AS mc 
               WHERE mc.movie_id = m.movie_id 
               ORDER BY mc.cast_order ASC 
               LIMIT 2)) AS top_2_cast_ids,
        
        -- top 5 cast members by billing order
        (SELECT GROUP_CONCAT(person_id) 
         FROM (SELECT mc.person_id 
               FROM movie_cast AS mc 
               WHERE mc.movie_id = m.movie_id 
               ORDER BY mc.cast_order ASC 
               LIMIT 5)) AS top_5_cast_ids,

        -- number of cast members
        (SELECT COUNT(*) 
         FROM movie_cast AS mc 
         WHERE mc.movie_id = m.movie_id) AS num_cast_members,
        
        -- directors
        (SELECT GROUP_CONCAT(mc.person_id) 
         FROM movie_crew AS mc 
         WHERE mc.movie_id = m.movie_id AND mc.job IN ('Director', 'Co-Director')) AS director_ids,

        -- directors
        (SELECT GROUP_CONCAT(mc.person_id) 
         FROM movie_crew AS mc 
         WHERE mc.movie_id = m.movie_id AND mc.department = 'Writing') AS writer_ids,
        
        -- production companies
        (SELECT GROUP_CONCAT(mpc.company_id) 
         FROM movie_production_company AS mpc 
         WHERE mpc.movie_id = m.movie_id) AS company_ids,

        -- genres
        (SELECT GROUP_CONCAT(mg.genre_id) 
         FROM movie_genre AS mg 
         WHERE mg.movie_id = m.movie_id) AS genre_ids
        
    FROM movie AS m
    WHERE m.vote_count >= 30;


CREATE VIEW movie_scores AS 
    SELECT 
        m.movie_id,
        (2 * AVG(umr.rating) * COUNT(umr.rating) + m.vote_average * m.vote_count) 
        / (COUNT(umr.rating) + m.vote_count) AS score,
        COUNT(umr.rating) + m.vote_count AS score_count
    FROM movie m
    JOIN movie_link ml ON m.movie_id = ml.tmdb_id
    JOIN user_movie_rating umr ON ml.movielens_id = umr.movielens_id
    WHERE m.vote_count > 0
    GROUP BY m.movie_id
    HAVING score_count >= 30;

