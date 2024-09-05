

with source as (

    select * from {{ source('WEETABIX', 'inventmodelgroupitem') }}

),

renamed as (

    select
        itemdataareaid,
        modelgroupid,
        itemid,
        modelgroupdataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
