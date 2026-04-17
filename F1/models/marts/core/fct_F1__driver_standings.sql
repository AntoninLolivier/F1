WITH standings AS (
    SELECT * FROM {{ ref('stg_F1__driver_standings') }}
),

races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

drivers AS (
    SELECT * FROM {{ ref('stg_F1__drivers') }}
),

final AS (

    SELECT
        s.driver_standing_sk,
        s.race_id,
        r.name,
        r.year,
        s.driver_id,
        {{ F1.first_name_last_name_to_full_name('d.first_name', 'd.last_name') }} as driver_full_name,
        s.total_points_accumulated,
        s.championship_position,
        s.disqualified,
        s.total_wins_season

    FROM standings s
        INNER JOIN races r
            ON s.race_id = r.race_id
        INNER JOIN drivers d
            ON s.driver_id = d.driver_id

)

SELECT * FROM final