WITH qualifyings AS (
    SELECT * FROM {{ ref('stg_F1__qualifying') }}
),

races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

drivers AS (
    SELECT * FROM {{ ref('stg_F1__drivers') }}
),

constructtors AS (
    SELECT * FROM {{ ref('stg_F1__constructors') }}
),

final AS (

    SELECT
        q.qualify_sk,
        q.race_id,
        r.name,
        r.year,
        q.driver_id,
        {{ F1.first_name_last_name_to_full_name('d.first_name', 'd.last_name') }} as driver_full_name,
        q.driver_number,
        q.constructor_id,
        c.name as constructor_name,
        q.qualifying_position,
        q.q1_time,
        q.q2_time,
        q.q3_time

    FROM qualifyings q
        INNER JOIN races r
            ON q.race_id = r.race_id
        INNER JOIN drivers d
            ON q.driver_id = d.driver_id
        INNER JOIN constructtors c
            ON q.constructor_id = c.constructor_id

)

SELECT * FROM final