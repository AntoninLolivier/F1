WITH races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

circuits AS (
    SELECT * FROM {{ ref('stg_F1__circuit') }}
),

final as (

    SELECT
        r.race_id,
        r.year,
        r.round,
        r.name,
        r.circuit_id,
        c.name as circuit_name,
        c.location,
        c.country,
        {{ generate_location_geography('c.longitude', 'c.latitude', 'c.altitude') }} AS location_geography,
        r.race_date_time_utc,
        r.free_practice_1_date_time_utc,
        r.free_practice_2_date_time_utc,
        r.free_practice_3_date_time_utc,
        r.qualifying_date_time_utc,
        r.sprint_date_time_utc

    FROM races r
        INNER JOIN circuits c
            ON r.circuit_id = c.circuit_id
)

SELECT * FROM final