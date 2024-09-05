

with source as (

    select * from {{ source('WEETABIX', 'inventitemgroupitem') }}

),

renamed as (

    select
        itemid,
        itemdataareaid,
        itemgroupid,
        itemgroupdataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
