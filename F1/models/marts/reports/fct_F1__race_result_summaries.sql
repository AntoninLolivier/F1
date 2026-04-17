WITH races AS (
    SELECT * FROM {{ ref('fct_F1__races') }}
),

lap_times AS (
    SELECT * FROM {{ ref('fct_F1__lap_times') }}
),

pit_stops AS (
    SELECT * FROM {{ ref('fct_F1__pit_stops') }}
),

driver_results AS (
    SELECT * FROM {{ ref('fct_F1__driver_results') }}
),

driver_standings AS (
    SELECT * FROM {{ ref('fct_F1__driver_standings') }}
),

constructor_standings AS (
    SELECT * FROM {{ ref('fct_F1__constructor_standings') }}
),

-------------------------------------------------------------------------
-- 1. Référentiels (Pour récupérer les noms sans recréer des jointures lourdes)
-------------------------------------------------------------------------
driver_names AS (
    -- On récupère la liste unique des pilotes avec leur nom complet
    SELECT DISTINCT driver_id, driver_full_name 
    FROM driver_standings
),

constructor_names AS (
    -- On récupère la liste unique des écuries avec leur nom
    SELECT DISTINCT constructor_id, constructor_name 
    FROM constructor_standings
),

-------------------------------------------------------------------------
-- 2. Calculs isolés (Un CTE par type de "Record")
-------------------------------------------------------------------------

-- A. Le Vainqueur
winner AS (
    SELECT 
        race_id,
        driver_id AS winner_driver_id,
        constructor_id AS winner_constructor_id,
        starting_grid_position AS winner_grid_position
    FROM driver_results
    WHERE final_order = 1
),

-- B. Le Meilleur Tour
fastest_lap AS (
    SELECT 
        race_id,
        driver_id AS fastest_lap_driver_id,
        driver_name AS fastest_lap_driver_name,
        lap_time_milliseconds AS fastest_lap_time_ms
    FROM lap_times
    -- QUALIFY agit comme un filtre WHERE mais sur les Window Functions. 
    -- On garde uniquement la ligne (le tour) avec le temps le plus bas par course.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY race_id ORDER BY lap_time_milliseconds ASC) = 1
),

-- C. La Vitesse Max (basée sur le fastest_lap_speed_kph)
max_speed AS (
    SELECT 
        race_id,
        driver_id AS max_speed_driver_id,
        fastest_lap_speed_kph AS max_speed_kph
    FROM driver_results
    WHERE fastest_lap_speed_kph IS NOT NULL
    -- On cast en FLOAT au cas où la source Ergast l'ait stocké en STRING
    QUALIFY ROW_NUMBER() OVER (PARTITION BY race_id ORDER BY fastest_lap_speed_kph DESC) = 1
),

-- D. Le Pit Stop le plus rapide
fastest_pit_stop AS (
    SELECT 
        ps.race_id,
        ps.driver_id AS fastest_pit_stop_driver_id,
        ps.driver_full_name AS fastest_pit_stop_driver_name,
        res.constructor_id AS fastest_pit_stop_constructor_id,
        ps.duration_milliseconds AS fastest_pit_stop_time_ms
    FROM pit_stops ps
    -- On a besoin de driver_results juste pour rattacher l'écurie au pit stop
    LEFT JOIN driver_results res 
        ON ps.race_id = res.race_id AND ps.driver_id = res.driver_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.race_id ORDER BY ps.duration_milliseconds ASC) = 1
),

-------------------------------------------------------------------------
-- 3. Assemblage Final
-------------------------------------------------------------------------
final AS (
    SELECT
        -- Contexte de la course
        r.race_id,
        r.year,
        r.name AS race_name,
        r.country,
        
        -- Vainqueur
        w.winner_driver_id,
        dn_w.driver_full_name AS winner_driver_name,
        w.winner_constructor_id,
        cn_w.constructor_name AS winner_constructor_name,
        w.winner_grid_position,

        -- Meilleur Tour
        fl.fastest_lap_driver_id,
        fl.fastest_lap_driver_name,
        fl.fastest_lap_time_ms,

        -- Vitesse Max
        ms.max_speed_driver_id,
        dn_ms.driver_full_name AS max_speed_driver_name,
        ms.max_speed_kph,

        -- Meilleur Arrêt au Stand
        fp.fastest_pit_stop_driver_id,
        fp.fastest_pit_stop_driver_name,
        fp.fastest_pit_stop_constructor_id,
        cn_fp.constructor_name AS fastest_pit_stop_constructor_name,
        fp.fastest_pit_stop_time_ms

    FROM races r
        -- On join tous nos blocs isolés au niveau "Course" (1 ligne = 1 course)
        LEFT JOIN winner w ON r.race_id = w.race_id
        LEFT JOIN fastest_lap fl ON r.race_id = fl.race_id
        LEFT JOIN max_speed ms ON r.race_id = ms.race_id
        LEFT JOIN fastest_pit_stop fp ON r.race_id = fp.race_id

        -- On enrichit avec les noms via nos dictionnaires uniques
        LEFT JOIN driver_names dn_w ON w.winner_driver_id = dn_w.driver_id
        LEFT JOIN constructor_names cn_w ON w.winner_constructor_id = cn_w.constructor_id
        LEFT JOIN driver_names dn_ms ON ms.max_speed_driver_id = dn_ms.driver_id
        LEFT JOIN constructor_names cn_fp ON fp.fastest_pit_stop_constructor_id = cn_fp.constructor_id
)

SELECT * FROM final