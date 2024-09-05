

with source as (

    select * from {{ source('WEETABIX', 'workperiodtemplateline') }}

),

renamed as (

    select
        periodtemplate,
        linenumber,
        period,
        numberofperiods,
        explodeperiods,
        perioddescription,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
