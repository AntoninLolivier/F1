WITH drivers AS (
    SELECT * FROM {{ ref('stg_F1__drivers') }}
),

standings AS (
    SELECT * FROM {{ ref('stg_F1__driver_standings') }}
),

races AS (
    SELECT * FROM {{ ref('stg_F1__races') }}
),

performances AS (
    SELECT * FROM {{ ref('int_F1__driver_performances_aggregated') }}
),

driver_races AS (
    SELECT
        r.race_id,
        s.driver_id,
        r.year
    FROM standings s
        LEFT JOIN races r
            ON s.race_id = r.race_id
),

first_years AS (
    SELECT
        driver_id,
        MIN(year) AS first_active_year
    FROM driver_races
    GROUP BY driver_id
),

final AS (
    SELECT
        d.* EXCLUDE(first_name, last_name),
        f.first_active_year,
        p.* EXCLUDE(driver_id)
    FROM drivers d
        LEFT JOIN first_years f
            ON d.driver_id = f.driver_id
        LEFT JOIN performances p
            ON d.driver_id = p.driver_id
)

select * from final