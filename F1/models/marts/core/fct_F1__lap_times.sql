WITH lap_times AS (
    SELECT * FROM {{ ref('stg_F1__lap_times') }}
),

races AS(
    SELECT * FROM {{ ref('stg_F1__races') }}
),

drivers AS (
    SELECT * FROM {{ ref('stg_F1__drivers') }}
),


final AS (

    SELECT
        lt.lap_time_sk,
        lt.race_id,
        r.name as race_name,
        r.year as race_year,
        lt.driver_id,
        {{ F1.first_name_last_name_to_full_name('d.first_name', 'd.last_name') }} as driver_name,
        lt.lap_number,
        lt.driver_position,
        lt.lap_time,
        lt.lap_time_milliseconds

    FROM lap_times lt
        INNER JOIN races r
            ON lt.race_id = r.race_id
        INNER JOIN drivers d
            ON lt.driver_id = d.driver_id

)

SELECT * FROM final