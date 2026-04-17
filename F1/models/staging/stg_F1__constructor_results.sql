with source as (

    select * from {{ source('F1', 'constructor_results') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['race_id', 'constructor_id']) }} as constructor_result_sk,
        race_id,
        constructor_id,
        points as points_earned,
        case when status = 'D' then true else false end as disqualified

    from source

)

select * from renamed