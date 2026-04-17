WITH results AS (
    SELECT * FROM {{ ref('stg_F1__results') }}
),

drivers AS (
    SELECT * FROM {{ ref('stg_F1__drivers') }}
),

standings AS (
    SELECT * FROM {{ ref('stg_F1__driver_standings') }}
),

races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

lap_times AS (
    SELECT * FROM {{ ref('stg_F1__lap_times') }}
),

pit_stops AS (
    SELECT * FROM {{ ref('stg_F1__pit_stops') }}
),

last_race_per_year AS (
    SELECT 
        year, 
        MAX(round) as last_round
    FROM {{ ref('stg_F1__races') }}
    GROUP BY 1
),

best_laps AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id']) }} as lap_sk,
        race_id,
        driver_id,
        MIN(lap_time_milliseconds) AS best_lap_ms 
    FROM lap_times 
    GROUP BY race_id, driver_id
),

best_pits AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id']) }} as pit_sk,
        race_id,
        driver_id,
        MIN(duration_milliseconds) AS best_pit_ms 
    FROM pit_stops
    GROUP BY race_id, driver_id
),

season_summaries AS (
    SELECT 
        res.driver_id,
        {{ first_name_last_name_to_full_name('d.first_name', 'd.last_name') }} AS driver_name,
        r.year,
        MAX(s.total_points_accumulated) AS points_in_season,
        MIN(s.championship_position) AS best_pos_reached_in_season,
        MAX(CASE WHEN r.round = lry.last_round THEN s.championship_position END) AS final_championship_position,
        MAX(s.total_wins_season) AS wins_in_season,
        MIN(bl.best_lap_ms) AS best_lap_ms_in_season,
        MIN(bp.best_pit_ms) AS best_pit_ms_in_season
    FROM results res
        INNER JOIN races r 
            ON res.race_id = r.race_id
        INNER JOIN last_race_per_year lry 
            ON r.year = lry.year
        INNER JOIN drivers d 
            ON res.driver_id = d.driver_id
        LEFT JOIN standings s 
            ON res.result_sk = s.driver_standing_sk
        LEFT JOIN best_laps bl 
            ON res.result_sk = bl.lap_sk
        LEFT JOIN best_pits bp 
            ON res.result_sk = bp.pit_sk
    WHERE res.is_classified = true 
    GROUP BY res.driver_id, driver_name, r.year
),

final AS (
    SELECT
        driver_id,
        driver_name,
        MAX(points_in_season) AS record_points_single_season,
        SUM(points_in_season) AS total_points_career,
        MIN(best_pos_reached_in_season) AS best_championship_position_ever,
        MAX(wins_in_season) AS record_wins_single_season,
        COUNT(CASE WHEN final_championship_position = 1 THEN 1 END) AS total_championships_titles,
        COUNT(CASE WHEN best_pos_reached_in_season = 1 THEN 1 END) AS seasons_led_championship,
        COUNT(CASE WHEN best_pos_reached_in_season <= 3 THEN 1 END) AS total_seasons_on_podium,
        MIN(best_lap_ms_in_season) AS best_lap_time_single_season,
        MIN(best_lap_ms_in_season) AS best_lap_time_milliseconds_single_season,
        MIN(best_pit_ms_in_season) AS best_pit_stop_duration_in_season
    FROM season_summaries
    GROUP BY driver_id, driver_name
)

SELECT * FROM final