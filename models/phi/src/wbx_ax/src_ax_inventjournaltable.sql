with
    source as (select * from {{ source("WEETABIX", "inventjournaltable") }}),

    renamed as (

        select
            journalid,
            description,
            posted,
            reservation,
            systemblocked,
            blockuserid,
            sessionlogindatetime,
            sessionlogindatetimetzid,
            posteddatetime,
            posteddatetimetzid,
            journaltype,
            journalnameid,
            inventdimfixed,
            blockusergroupid,
            voucherdraw,
            voucherchange,
            sessionid,
            posteduserid,
            numoflines,
            journalidorignal,
            detailsummary,
            deletepostedlines,
            ledgerdimension,
            worker,
            vouchernumbersequencetable,
            storno_ru,
            offsessionid_ru,
            retailreplenishmenttype,
            fshreplenishmentref,
            retailretailstatustype,
            inventdoctype_pl,
            inventlocationid,
            inventsiteid,
            source,
            dataareaid,
            recversion,
            partition,
            recid,
            modifieddatetime,
            modifiedby,
            createddatetime,
            createdby

        from source

    )

select *
from renamed
