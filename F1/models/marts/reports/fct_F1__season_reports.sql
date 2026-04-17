WITH races AS (
    SELECT * FROM {{ ref('fct_F1__races') }}
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

race_summaries AS (
    SELECT * FROM {{ ref('fct_F1__race_result_summaries') }}
),

-------------------------------------------------------------------------
-- 1. Identifier la dernière course de chaque saison (pour les titres)
-------------------------------------------------------------------------
last_race_per_year AS (
    SELECT 
        year, 
        race_id
    FROM races
    -- On garde uniquement le dernier round de l'année
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY round DESC) = 1
),

-------------------------------------------------------------------------
-- 2. Le Podium Pilotes (1er, 2ème, 3ème)
-------------------------------------------------------------------------
final_driver_standings AS (
    SELECT 
        r.year,
        ds.driver_id,
        ds.driver_full_name,
        -- Pour récupérer l'écurie de fin d'année du pilote, on joint ses résultats de la dernière course
        dr.constructor_id, 
        ds.total_points_accumulated AS points,
        ds.championship_position
    FROM driver_standings ds
        INNER JOIN last_race_per_year lry ON ds.race_id = lry.race_id
        INNER JOIN races r ON lry.race_id = r.race_id
        LEFT JOIN driver_results dr ON lry.race_id = dr.race_id AND ds.driver_id = dr.driver_id
    WHERE ds.championship_position <= 3
),

-- On "aplatit" les 3 lignes du podium en 1 seule ligne par année (Wide Format)
driver_podiums AS (
    SELECT 
        year,
        MAX(CASE WHEN championship_position = 1 THEN driver_id END) AS first_place_driver_id,
        MAX(CASE WHEN championship_position = 1 THEN driver_full_name END) AS first_place_driver_name,
        MAX(CASE WHEN championship_position = 1 THEN constructor_id END) AS first_place_constructor_id,
        MAX(CASE WHEN championship_position = 1 THEN points END) AS first_place_points,

        MAX(CASE WHEN championship_position = 2 THEN driver_id END) AS second_place_driver_id,
        MAX(CASE WHEN championship_position = 2 THEN driver_full_name END) AS second_place_driver_name,
        MAX(CASE WHEN championship_position = 2 THEN constructor_id END) AS second_place_constructor_id,
        MAX(CASE WHEN championship_position = 2 THEN points END) AS second_place_points,

        MAX(CASE WHEN championship_position = 3 THEN driver_id END) AS third_place_driver_id,
        MAX(CASE WHEN championship_position = 3 THEN driver_full_name END) AS third_place_driver_name,
        MAX(CASE WHEN championship_position = 3 THEN constructor_id END) AS third_place_constructor_id,
        MAX(CASE WHEN championship_position = 3 THEN points END) AS third_place_points
    FROM final_driver_standings
    GROUP BY year
),

-------------------------------------------------------------------------
-- 3. Le Titre Constructeur
-------------------------------------------------------------------------
constructor_champions AS (
    SELECT 
        r.year,
        cs.constructor_id AS world_constructor_champion_id,
        cs.constructor_name AS world_constructor_champion_name,
        cs.total_points_accumulated AS world_constructor_points
    FROM constructor_standings cs
        INNER JOIN last_race_per_year lry ON cs.race_id = lry.race_id
        INNER JOIN races r ON lry.race_id = r.race_id
    WHERE cs.championship_position = 1
    -- QUALIFY au cas où deux écuries seraient ex-aequo (sécurité analytique)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.year ORDER BY cs.total_points_accumulated DESC) = 1
),

-------------------------------------------------------------------------
-- 4. Les Records de la Saison (Vitesse, Tour, Pit Stop) depuis notre résumé
-------------------------------------------------------------------------
season_max_speed AS (
    SELECT 
        year, max_speed_driver_id, max_speed_kph, race_id
    FROM race_summaries
    WHERE max_speed_kph IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY max_speed_kph::FLOAT DESC) = 1
),

season_fastest_lap AS (
    SELECT 
        year, fastest_lap_driver_id, fastest_lap_time_ms, race_id
    FROM race_summaries
    WHERE fastest_lap_time_ms IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY fastest_lap_time_ms ASC) = 1
),

season_fastest_pit_stop AS (
    SELECT 
        year, fastest_pit_stop_driver_id, fastest_pit_stop_constructor_id, fastest_pit_stop_time_ms, race_id
    FROM race_summaries
    WHERE fastest_pit_stop_time_ms IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY fastest_pit_stop_time_ms ASC) = 1
),

-------------------------------------------------------------------------
-- 5. Le Remplacement : Pilote avec le plus de victoires dans la saison
-------------------------------------------------------------------------
most_wins_driver AS (
    SELECT 
        r.year,
        dr.driver_id AS driver_most_wins_id,
        COUNT(dr.result_sk) AS max_wins_count
    FROM driver_results dr
        INNER JOIN races r ON dr.race_id = r.race_id
    WHERE dr.final_order = 1
    GROUP BY r.year, dr.driver_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.year ORDER BY max_wins_count DESC) = 1
),

-------------------------------------------------------------------------
-- 6. Assemblage Final par Année (Grain = 1 ligne par Saison)
-------------------------------------------------------------------------
years AS (
    SELECT DISTINCT year FROM races
),

final AS (
    SELECT 
        y.year AS season_year,
        
        -- Titre Constructeur
        cc.world_constructor_champion_id,
        cc.world_constructor_champion_name,
        cc.world_constructor_points,

        -- Podium Pilotes
        dp.first_place_driver_id,
        dp.first_place_driver_name,
        dp.first_place_constructor_id,
        dp.first_place_points,
        dp.second_place_driver_id,
        dp.second_place_driver_name,
        dp.second_place_constructor_id,
        dp.second_place_points,
        dp.third_place_driver_id,
        dp.third_place_driver_name,
        dp.third_place_constructor_id,
        dp.third_place_points,

        -- Most Wins (Remplacement de fastest_race_time)
        mw.driver_most_wins_id,
        mw.max_wins_count,

        -- Record de Vitesse Max
        sms.max_speed_driver_id,
        sms.max_speed_kph AS season_max_speed_kph,
        sms.race_id AS max_speed_race_id,
        r_ms.circuit_id AS max_speed_circuit_id,
        r_ms.circuit_name AS max_speed_circuit_name, -- Attention à l'alias dans fct_races !

        -- Record du Meilleur Tour
        sfl.fastest_lap_driver_id,
        sfl.fastest_lap_time_ms AS season_fastest_lap_ms,
        sfl.race_id AS fastest_lap_race_id,
        r_fl.circuit_id AS fastest_lap_circuit_id,
        r_fl.circuit_name AS fastest_lap_circuit_name,

        -- Record du Meilleur Arrêt
        sfp.fastest_pit_stop_driver_id,
        sfp.fastest_pit_stop_constructor_id,
        sfp.fastest_pit_stop_time_ms AS season_fastest_pit_stop_ms,
        sfp.race_id AS fastest_pit_stop_race_id,
        r_fp.circuit_id AS fastest_pit_stop_circuit_id,
        r_fp.circuit_name AS fastest_pit_stop_circuit_name

    FROM years y
        LEFT JOIN driver_podiums dp ON y.year = dp.year
        LEFT JOIN constructor_champions cc ON y.year = cc.year
        LEFT JOIN most_wins_driver mw ON y.year = mw.year
        
        -- Jointures pour les records et le contexte géographique (via races)
        LEFT JOIN season_max_speed sms ON y.year = sms.year
        LEFT JOIN races r_ms ON sms.race_id = r_ms.race_id

        LEFT JOIN season_fastest_lap sfl ON y.year = sfl.year
        LEFT JOIN races r_fl ON sfl.race_id = r_fl.race_id

        LEFT JOIN season_fastest_pit_stop sfp ON y.year = sfp.year
        LEFT JOIN races r_fp ON sfp.race_id = r_fp.race_id
)

SELECT * FROM final
ORDER BY season_year DESC