with source as (

    select * from {{ source('F1', 'status') }}

),

renamed as (

    select
        status_id,
        status

    from source

)

select * from renamed