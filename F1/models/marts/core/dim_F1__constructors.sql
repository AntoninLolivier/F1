WITH constructors AS (

    SELECT * FROM {{ ref('stg_F1__constructors') }}
),

performances AS (

    SELECT * FROM {{ ref('int_F1__constructor_performances_aggregated') }}

),

constructor_perfs AS (
    SELECT 
        c.constructor_id,
        c.constructor_ref,
        c.name,
        c.nationality,
        p.record_points_single_season,
        p.total_points_career,
        p.best_championship_position_ever,
        p.record_wins_single_season,
        p.total_championships_titles,
        p.total_seasons_on_podium
    FROM constructors c
        LEFT JOIN performances p 
            ON c.constructor_id = p.constructor_id
),

final AS (

    SELECT * FROM constructor_perfs
)

SELECT * FROM final