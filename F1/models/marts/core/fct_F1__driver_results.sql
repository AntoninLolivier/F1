WITH results AS (
    SELECT * FROM {{ ref('stg_F1__results') }}
),

races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

statuses AS (
    SELECT * FROM {{ ref('stg_F1__status') }}
),

final AS (
    SELECT
        res.result_sk,
        res.race_id,
        r.name,
        res.driver_id,
        res.constructor_id,
        res.driver_number,
        res.starting_grid_position,
        res.displayed_final_position,
        res.final_order,
        CASE 
            WHEN res.is_classified = true 
                THEN res.starting_grid_position - res.displayed_final_position::NUMBER
            ELSE NULL
        END AS position_gain,
        res.is_classified,
        {{ decode_f1_status('res.do_not_finish_reason') }} as do_not_finish_reason,
        res.points_earned,
        res.laps_completed,
        res.race_time,
        res.race_time_milliseconds,
        res.fastest_lap_number,
        res.fastest_lap_rank,
        res.fastest_lap_time,
        res.fastest_lap_speed_kph,
        s.status
    FROM results res
        INNER JOIN races r
            ON r.race_id = res.race_id
        LEFT JOIN statuses s 
            ON res.status_id = s.status_id
)

SELECT * FROM final