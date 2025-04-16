WITH 
OverallAverage AS (
    SELECT AVG(vote_average) AS overall_avg_vote
    FROM movie
    WHERE vote_count >= 30
),
PastDirectorMovies AS (
    SELECT 
        m.movie_id,
        pm.vote_average AS past_vote_average,
        JULIANDAY(m.release_date) - JULIANDAY(pm.release_date) AS days_difference
    FROM movie m
    JOIN movie_crew mc1 ON m.movie_id = mc1.movie_id  
    JOIN movie_crew mc2 ON mc1.person_id = mc2.person_id  
    JOIN movie pm ON mc2.movie_id = pm.movie_id  
    WHERE mc1.job IN ('Director', 'Co-Director') 
      AND mc2.job IN ('Director', 'Co-Director')
      AND pm.release_date < m.release_date
      AND pm.vote_count >= 30  
),
DirectorScores AS (
    SELECT 
        movie_id, 
        SUM(past_vote_average * EXP(-? * days_difference)) / SUM(EXP(-? * days_difference)) AS director_score
    FROM PastDirectorMovies
    GROUP BY movie_id
),
PastWriterMovies AS (
    SELECT 
        m.movie_id,
        pm.vote_average AS past_vote_average,
        JULIANDAY(m.release_date) - JULIANDAY(pm.release_date) AS days_difference
    FROM movie m
    JOIN movie_crew mc1 ON m.movie_id = mc1.movie_id  
    JOIN movie_crew mc2 ON mc1.person_id = mc2.person_id  
    JOIN movie pm ON mc2.movie_id = pm.movie_id  
    WHERE mc1.department = 'Writing' 
      AND mc2.department = 'Writing'
      AND pm.release_date < m.release_date
      AND pm.vote_count >= 30  
),
WriterScores AS (
    SELECT 
        movie_id, 
        SUM(past_vote_average * EXP(-? * days_difference)) / SUM(EXP(-? * days_difference)) AS writer_score
    FROM PastWriterMovies
    GROUP BY movie_id
),
PastCastMovies AS (
    SELECT 
        m.movie_id,
        prev_m.vote_average AS past_vote_average,
        JULIANDAY(m.release_date) - JULIANDAY(prev_m.release_date) AS days_difference,
        rc.cast_order
    FROM movie m
    JOIN movie_cast rc ON m.movie_id = rc.movie_id
    JOIN movie_cast prev_mc ON rc.person_id = prev_mc.person_id
    JOIN movie prev_m ON prev_mc.movie_id = prev_m.movie_id
    WHERE prev_m.release_date < m.release_date 
      AND prev_m.vote_count >= 30  
      AND rc.cast_order <= 10 -- Consider only top 10 billed actors
),
CastScores AS (
    SELECT 
        movie_id,
        CASE 
            WHEN SUM(EXP(-? * days_difference) * EXP(-? * cast_order)) = 0 
            THEN NULL
            ELSE SUM(past_vote_average * EXP(-? * days_difference) * EXP(-? * cast_order)) / 
                 SUM(EXP(-? * days_difference) * EXP(-? * cast_order))
        END AS cast_score
    FROM PastCastMovies
    GROUP BY movie_id
),
PastProductionMovies AS (
    SELECT 
        m.movie_id,
        prev_mpc.company_id,
        prev_m.vote_average AS past_vote_average
    FROM movie m
    JOIN movie_production_company mpc ON m.movie_id = mpc.movie_id
    JOIN movie_production_company prev_mpc ON mpc.company_id = prev_mpc.company_id
    JOIN movie prev_m ON prev_mpc.movie_id = prev_m.movie_id
    WHERE prev_m.release_date < m.release_date
      AND prev_m.vote_count >= 30
),
ProductionCompanyScores AS (
    SELECT 
        movie_id,
        AVG(past_vote_average) AS production_company_score
    FROM PastProductionMovies
    GROUP BY movie_id
)
SELECT 
    m.movie_id,
    COALESCE(ds.director_score, overall_avg_vote) AS director_score,
    COALESCE(ws.writer_score, overall_avg_vote) AS writer_score,
    COALESCE(cs.cast_score, overall_avg_vote) AS cast_score,
    COALESCE(pcs.production_company_score, overall_avg_vote) AS production_company_score
FROM OverallAverage, movie m
     LEFT JOIN DirectorScores ds ON m.movie_id = ds.movie_id
     LEFT JOIN WriterScores ws ON m.movie_id = ws.movie_id
     LEFT JOIN CastScores cs ON m.movie_id = cs.movie_id
     LEFT JOIN ProductionCompanyScores pcs ON m.movie_id = pcs.movie_id
WHERE m.vote_count >= ?;
