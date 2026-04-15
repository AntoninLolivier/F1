with source as (

    select * from {{ source('F1', 'qualifying') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id']) }} as qualify_sk,
        race_id,
        driver_id,
        constructor_id,
        number as driver_number,
        position as qualifying_position,
        try_to_time(q1, 'MI:SS.FF3') as q1_time,
        try_to_time(q2, 'MI:SS.FF3') as q2_time,
        try_to_time(q3, 'MI:SS.FF3') as q3_time

    from source

)

select * from renamed