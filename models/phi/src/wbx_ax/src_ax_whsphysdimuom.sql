

with source as (

    select * from {{ source('WEETABIX', 'whsphysdimuom') }}

),

renamed as (

    select
        uom,
        itemid,
        physdimid,
        depth,
        height,
        weight,
        width,
        modifieddatetime,
        modifiedby,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
