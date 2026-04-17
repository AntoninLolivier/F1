WITH standings AS (
    SELECT * FROM {{ ref('stg_F1__constructor_standings') }}
),

races AS (
    SELECT * FROM {{ ref ('stg_F1__races') }}
),

constructors AS (
    SELECT * FROM {{ ref('stg_F1__constructors') }}
),

final AS (

    SELECT
        constructor_standing_sk,
        s.constructor_id,
        c.name as constructor_name,
        s.race_id,
        r.name as race_name,
        r.year as race_year,
        s.total_points_accumulated,
        s.championship_position,
        s.excluded,
        s.total_wins_season
    FROM standings s
        INNER JOIN races r
            ON s.race_id = r.race_id
        INNER JOIN constructors c
            ON s.constructor_id = c.constructor_id

)

SELECT * FROM final