WITH results AS (
    SELECT * FROM {{ ref('stg_F1__constructor_results') }}
),

races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

constructors AS (
    SELECT * FROM {{ ref('stg_F1__constructors') }}
),

final AS (

    SELECT
        res.constructor_result_sk,
        res.race_id,
        r.name as race_name,
        r.year,
        res.constructor_id,
        c.name as constructor_name,
        res.points_earned,
        res.disqualified

    FROM results res
        INNER JOIN races r
            ON r.race_id = res.race_id
        INNER JOIN constructors c
            ON c.constructor_id = res.constructor_id

)

SELECT * FROM final