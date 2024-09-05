

with source as (

    select * from {{ source('D365', 'work_period_template') }}

),

renamed as (

    select
        legal_entity as legalentity,
        name as name,
        fixed_day_start as fixeddaystart,
        work_time_id as worktimeid,
        upper(work_time_id_data_area_id) as worktimeiddataareaid,
        legal_entity_default as legalentitydefault,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
