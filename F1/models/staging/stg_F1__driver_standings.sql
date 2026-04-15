with source as (

    select * from {{ source('F1', 'driver_standings') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id']) }} as driver_standing_sk,
        race_id,
        driver_id,
        points as total_points_accumulated,
        position as championship_position,
        position_text = 'D' as disqualified,
        wins as total_wins_season

    from source

)

select * from renamed