with source as (

    select * from {{ source('F1', 'races') }}

),

renamed as (

    select
        race_id,
        year,
        round,
        circuit_id,
        name,
        timestamp_ntz_from_parts(date, time) as race_date_time_utc,
        timestamp_ntz_from_parts(fp1_date, fp1_time) as free_practice_1_date_time_utc,
        timestamp_ntz_from_parts(fp2_date, fp2_time) as free_practice_2_date_time_utc,
        timestamp_ntz_from_parts(fp3_date, fp3_time) as free_practice_3_date_time_utc,
        timestamp_ntz_from_parts(quali_date, quali_time) as qualifying_date_time_utc,
        timestamp_ntz_from_parts(sprint_date, sprint_time) as sprint_date_time_utc

    from source

)

select * from renamed