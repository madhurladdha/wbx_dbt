

with source as (

    select * from {{ source('WEETABIX', 'reqplanversion') }}

),

renamed as (

    select
        active,
        reqplandataareaid,
        reqplanid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
