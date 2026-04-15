with source as (

    select * from {{ source('F1', 'pit_stops') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id', 'stop']) }} as pit_stop_sk,
        race_id,
        driver_id,
        stop as stop_number,
        lap as lap_number,
        time as pit_stop_time,
        milliseconds as duration_milliseconds

    from source

)

select * from renamed