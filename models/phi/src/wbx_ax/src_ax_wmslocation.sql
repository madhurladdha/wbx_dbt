

with source as (

    select * from {{ source('WEETABIX', 'wmslocation') }}

),

renamed as (

    select
        position,
        level_,
        rack,
        inventlocationid,
        wmslocationid,
        checktext,
        sortcode,
        manualsortcode,
        manualname,
        maxvolume,
        aisleid,
        maxweight,
        locationtype,
        height,
        width,
        depth,
        volume,
        pallettypegroupid,
        storeareaid,
        maxpalletcount,
        inputlocation,
        inputblockingcauseid,
        outputblockingcauseid,
        pickingareaid,
        absoluteheight,
        locprofileid,
        zoneid,
        mcrreservationpriority,
        lastcountedutcdatetime,
        lastcountedutcdatetimetzid,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
