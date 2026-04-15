with source as (

    select * from {{ source('F1', 'circuit') }}

),

renamed as (

    select
        circuit_id,
        circuit_ref,
        name,
        location,
        country,
        lat as latitude,
        lng as longitude,
        alt as altitude

    from source

)

select * from renamed