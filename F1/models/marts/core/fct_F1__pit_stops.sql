WITH pit_stops AS (
    SELECT * FROM {{ ref('stg_F1__pit_stops') }}
),

races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

drivers AS (
    SELECT * FROM {{ ref('stg_F1__drivers') }}
),

final AS (

    SELECT
        ps.pit_stop_sk,
        ps.race_id,
        r.name,
        r.year,
        ps.driver_id,
        {{ F1.first_name_last_name_to_full_name('d.first_name', 'd.last_name') }} as driver_full_name,
        ps.stop_number,
        ps.lap_number,
        ps.pit_stop_time,
        ps.duration_milliseconds

    FROM pit_stops ps
        INNER JOIN races r
            ON ps.race_id = r.race_id
        INNER JOIN drivers d
            ON ps.driver_id = d.driver_id
)

SELECT * FROM final