

with source as (

    select * from {{ source('WEETABIX', 'inventdim') }}

),

renamed as (

    select
        inventdimid,
        inventbatchid,
        wmslocationid,
        wmspalletid,
        inventserialid,
        inventlocationid,
        configid,
        inventsizeid,
        inventcolorid,
        inventsiteid,
        inventgtdid_ru,
        inventprofileid_ru,
        inventownerid_ru,
        inventstyleid,
        licenseplateid,
        inventstatusid,
        sha1hash,
        modifieddatetime,
        modifiedby,
        createddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
