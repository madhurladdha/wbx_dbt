

with source as (

    select * from {{ source('WEETABIX', 'inventstrategiccode') }}

),

renamed as (

    select
        strategiccodeid,
        strategicdescr,
        strategicseq,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
