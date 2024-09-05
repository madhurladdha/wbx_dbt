

with source as (

    select * from {{ source('WEETABIX', 'projsorting') }}

),

renamed as (

    select
        sortingid,
        description,
        sortcode,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
