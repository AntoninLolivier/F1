WITH results AS (
    SELECT * FROM {{ ref('stg_F1__constructor_results') }}
),

constructors AS (
    SELECT * FROM {{ ref('stg_F1__constructors') }}
),

standings AS (
    SELECT * FROM {{ ref('stg_F1__constructor_standings') }}
),

races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

season_summaries AS (
    SELECT 
        res.constructor_id,
        c.name as constructor_name,
        r.year,
        MAX(s.total_points_accumulated) AS points_in_season,
        MIN(s.championship_position) AS best_pos_in_season,         
        MAX(s.total_wins_season) AS wins_in_season
    FROM results res
        LEFT JOIN standings s 
            ON res.constructor_result_sk = s.constructor_standing_sk 
        LEFT JOIN races r 
            ON res.race_id = r.race_id
        LEFT JOIN constructors c 
            ON res.constructor_id = c.constructor_id
            AND s.excluded = false
    WHERE res.disqualified = false 
    GROUP BY res.constructor_id, c.name, r.year
),

final AS (
    SELECT
        constructor_id,
        constructor_name,
        MAX(points_in_season) AS record_points_single_season,
        SUM(points_in_season) AS total_points_career,
        MIN(best_pos_in_season) AS best_championship_position_ever,
        MAX(wins_in_season) AS record_wins_single_season,
        COUNT(CASE WHEN best_pos_in_season = 1 THEN 1 END) AS total_championships_titles,
        COUNT(CASE WHEN best_pos_in_season <= 3 THEN 1 END) AS total_seasons_on_podium
    FROM season_summaries
    GROUP BY constructor_id, constructor_name
)

SELECT * FROM final