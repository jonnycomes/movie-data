DROP VIEW IF EXISTS movie_rating_features;

CREATE VIEW movie_rating_features AS
SELECT 
    m.movie_id,
    m.vote_average,
    m.title,
    m.release_date,
    m.runtime,
    
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

