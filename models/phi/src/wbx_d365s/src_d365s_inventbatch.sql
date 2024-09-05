with
d365_source as (
    select *
    from {{ source("D365S", "inventbatch") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        inventbatchid as inventbatchid,
        cast(expdate as TIMESTAMP_NTZ) as expdate,
        itemid as itemid,
        cast(proddate as TIMESTAMP_NTZ) as proddate,
        null as description,
        cast(pdsbestbeforedate as TIMESTAMP_NTZ) as pdsbestbeforedate,
        null as pdscountryoforigin1,
        null as pdscountryoforigin2,
        pdsdispositioncode as pdsdispositioncode,
        cast(pdsfinishedgoodsdatetested as TIMESTAMP_NTZ)
            as pdsfinishedgoodsdatetested,
        pdsinheritbatchattrib as pdsinheritbatchattrib,
        pdsinheritedshelflife as pdsinheritedshelflife,
        pdssamelot as pdssamelot,
        cast(pdsshelfadvicedate as TIMESTAMP_NTZ) as pdsshelfadvicedate,
        pdsusevendbatchdate as pdsusevendbatchdate,
        pdsusevendbatchexp as pdsusevendbatchexp,
        cast(pdsvendbatchdate as TIMESTAMP_NTZ) as pdsvendbatchdate,
        null as pdsvendbatchid,
        cast(pdsvendexpirydate as TIMESTAMP_NTZ) as pdsvendexpirydate,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
