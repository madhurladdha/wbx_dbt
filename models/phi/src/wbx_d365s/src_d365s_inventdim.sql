
with d365source as (
    select *
    from {{ source("D365S", "inventdim") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        inventdimid as inventdimid,
        inventbatchid as inventbatchid,
        wmslocationid as wmslocationid,
        null as wmspalletid,
        null as inventserialid,
        inventlocationid as inventlocationid,
        configid as configid,
        inventsizeid as inventsizeid,
        null as inventcolorid,
        inventsiteid as inventsiteid,
        null as inventgtdid_ru,
        null as inventprofileid_ru,
        null as inventownerid_ru,
        null as inventstyleid,
        licenseplateid as licenseplateid,
        inventstatusid as inventstatusid,
        null as sha1hash,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365source

)

select *
from renamed
