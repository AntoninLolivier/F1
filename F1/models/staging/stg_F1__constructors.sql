with source as (

    select * from {{ source('F1', 'constructors') }}

),

renamed as (

    select
        constructor_id,
        constructor_ref,
        name,
        nationality

    from source

)

select * from renamed