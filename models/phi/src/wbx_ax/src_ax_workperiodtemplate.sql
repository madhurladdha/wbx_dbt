

with source as (

    select * from {{ source('WEETABIX', 'workperiodtemplate') }}

),

renamed as (

    select
        legalentity,
        name,
        fixeddaystart,
        worktimeid,
        worktimeiddataareaid,
        legalentitydefault,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
