with source as (

    select * from {{ source('F1', 'constructor_standings') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['race_id', 'constructor_id']) }} as constructor_standing_sk,
        race_id,
        constructor_id,
        points as total_points_accumulated,
        position as championship_position,
        position_text = 'E' as excluded,
        wins as total_wins_season

    from source

)

select * from renamed