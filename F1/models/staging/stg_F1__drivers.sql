with source as (

    select * from {{ source('F1', 'drivers') }}

),

renamed as (

    select
        driver_id,
        driver_ref,
        number,
        code,
        forename as first_name,
        surname as last_name,
        dob as date_of_birth,
        nationality,

    from source

)

select * from renamed