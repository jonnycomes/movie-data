WITH OverallAverage AS (
    SELECT AVG(score) AS overall_avg_score
    FROM movie_scores
),
PastDirectorMovies AS (
    SELECT 
        m.movie_id,
        past_ms.score AS past_movie_score,
        JULIANDAY(m.release_date) - JULIANDAY(past_m.release_date) AS days_difference
    FROM movie m
    JOIN movie_crew mc1 ON m.movie_id = mc1.movie_id  
    JOIN movie_crew mc2 ON mc1.person_id = mc2.person_id  
    JOIN movie past_m ON mc2.movie_id = past_m.movie_id  
    JOIN movie_scores past_ms ON past_m.movie_id = past_ms.movie_id  
    WHERE mc1.job IN ('Director', 'Co-Director') 
      AND mc2.job IN ('Director', 'Co-Director')
      AND past_m.release_date < m.release_date
),
DirectorScores AS (
    SELECT 
        movie_id, 
        SUM(past_movie_score * EXP(-? * days_difference)) / SUM(EXP(-? * days_difference)) AS director_score
    FROM PastDirectorMovies
    GROUP BY movie_id
),
PastWriterMovies AS (
    SELECT 
        m.movie_id,
        past_ms.score AS past_movie_score,
        JULIANDAY(m.release_date) - JULIANDAY(past_m.release_date) AS days_difference
    FROM movie m
    JOIN movie_crew mc1 ON m.movie_id = mc1.movie_id  
    JOIN movie_crew mc2 ON mc1.person_id = mc2.person_id  
    JOIN movie past_m ON mc2.movie_id = past_m.movie_id  
    JOIN movie_scores past_ms ON past_m.movie_id = past_ms.movie_id  
    WHERE mc1.department = 'Writing' 
      AND mc2.department = 'Writing'
      AND past_m.release_date < m.release_date
),
WriterScores AS (
    SELECT 
        movie_id, 
        SUM(past_movie_score * EXP(-? * days_difference)) / SUM(EXP(-? * days_difference)) AS writer_score
    FROM PastWriterMovies
    GROUP BY movie_id
),
PastCastMovies AS (
    SELECT 
        m.movie_id,
        past_ms.score AS past_movie_score,
        JULIANDAY(m.release_date) - JULIANDAY(past_m.release_date) AS days_difference,
        mc.cast_order
    FROM movie m
    JOIN movie_cast mc ON m.movie_id = mc.movie_id
    JOIN movie_cast past_mc ON mc.person_id = past_mc.person_id
    JOIN movie past_m ON past_mc.movie_id = past_m.movie_id
    JOIN movie_scores past_ms ON past_m.movie_id = past_ms.movie_id
    WHERE past_m.release_date < m.release_date 
      AND mc.cast_order <= 10 -- Consider only top 10 billed actors
),
CastScores AS (
    SELECT 
        movie_id,
        CASE 
            WHEN SUM(EXP(-? * days_difference) * EXP(-? * cast_order)) = 0 
            THEN NULL
            ELSE SUM(past_movie_score * EXP(-? * days_difference) * EXP(-? * cast_order)) / 
                 SUM(EXP(-? * days_difference) * EXP(-? * cast_order))
        END AS cast_score
    FROM PastCastMovies
    GROUP BY movie_id
),
PastProductionMovies AS (
    SELECT 
        m.movie_id,
        past_ms.score AS past_movie_score
    FROM movie m
    JOIN movie_production_company mpc ON m.movie_id = mpc.movie_id
    JOIN movie_production_company past_mpc ON mpc.company_id = past_mpc.company_id
    JOIN movie past_m ON past_mpc.movie_id = past_m.movie_id
    JOIN movie_scores past_ms ON past_m.movie_id = past_ms.movie_id
    WHERE past_m.release_date < m.release_date
),
ProductionCompanyScores AS (
    SELECT 
        movie_id,
        AVG(past_movie_score) AS production_company_score
    FROM PastProductionMovies
    GROUP BY movie_id
)
SELECT 
    m.movie_id,
    COALESCE(ds.director_score, overall_avg_score) AS director_score,
    COALESCE(ws.writer_score, overall_avg_score) AS writer_score,
    COALESCE(cs.cast_score, overall_avg_score) AS cast_score,
    COALESCE(pcs.production_company_score, overall_avg_score) AS production_company_score
FROM OverallAverage, movie m
     LEFT JOIN DirectorScores ds ON m.movie_id = ds.movie_id
     LEFT JOIN WriterScores ws ON m.movie_id = ws.movie_id
     LEFT JOIN CastScores cs ON m.movie_id = cs.movie_id
     LEFT JOIN ProductionCompanyScores pcs ON m.movie_id = pcs.movie_id
WHERE m.vote_count >= 30;
