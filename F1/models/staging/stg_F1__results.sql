with source as (

    select * from {{ source('F1', 'results') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id']) }} as result_sk,
        race_id,
        driver_id,
        constructor_id,
        number as driver_number,
        grid as starting_grid_position,
        position as displayed_final_position,
        position_order as final_order,
        position is not null as is_classified, 
        case when TRY_TO_NUMBER(position_text) is not null
            then '+90%'
            else position_text
        end as do_not_finish_reason, 
        points as points_earned,
        laps as laps_completed,
        timeadd(ms, milliseconds, '00:00:00.000'::TIME) as race_time,
        milliseconds as race_time_milliseconds,
        fastest_lap as fastest_lap_number,
        rank as fastest_lap_rank,
        try_to_time(fastest_lap_time, 'MI:SS.FF3') as fastest_lap_time,
        fastest_lap_speed as fastest_lap_speed_kph,
        status_id

    from source

)

select * from renamed