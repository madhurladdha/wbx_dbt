with
d365_source as (
    select *
    from {{ source("D365S", "inventjournaltable") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (



    select
        'D365S' as source,
        journalid as journalid,
        description as description,
        posted as posted,
        reservation as reservation,
        systemblocked as systemblocked,
        null as blockuserid,
        cast(sessionlogindatetime as TIMESTAMP_NTZ) as sessionlogindatetime,
        --tzid values set to null
        null as sessionlogindatetimetzid,
        cast(posteddatetime as TIMESTAMP_NTZ) as posteddatetime,
        null as posteddatetimetzid,
        journaltype as journaltype,
        journalnameid as journalnameid,
        inventdimfixed as inventdimfixed,
        null as blockusergroupid,
        voucherdraw as voucherdraw,
        voucherchange as voucherchange,
        sessionid as sessionid,
        posteduserid as posteduserid,
        numoflines as numoflines,
        journalidorignal as journalidorignal,
        detailsummary as detailsummary,
        deletepostedlines as deletepostedlines,
        ledgerdimension as ledgerdimension,
        worker as worker,
        vouchernumbersequencetable as vouchernumbersequencetable,
        storno_ru as storno_ru,
        null as offsessionid_ru,
        retailreplenishmenttype as retailreplenishmenttype,
        null as fshreplenishmentref,
        retailretailstatustype as retailretailstatustype,
        inventdoctype_pl as inventdoctype_pl,
        inventlocationid as inventlocationid,
        inventsiteid as inventsiteid,
        null as source_data,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as modifieddatetime,
        null as modifiedby,
        null as createddatetime,
        null as createdby

    from d365_source

)

select * from renamed
