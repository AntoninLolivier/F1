WITH circuits AS (
    SELECT * FROM {{ ref('stg_F1__circuit') }}
),

final AS (
    SELECT 
        circuit_id,
        circuit_ref,
        name,
        {{ generate_location_geography('longitude', 'latitude', 'altitude') }} AS location_geography,
        location,
        country
    FROM circuits
)

SELECT * FROM final