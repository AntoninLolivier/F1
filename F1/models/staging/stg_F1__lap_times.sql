with source as (

    select * from {{ source('F1', 'lap_times') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id', 'lap']) }} as lap_time_sk,
        race_id,
        driver_id,
        lap as lap_number,
        position as driver_position,
        try_to_time(time, 'MI:SS.FF3') as lap_time,
        milliseconds as lap_time_milliseconds

    from source

)

select * from renamed