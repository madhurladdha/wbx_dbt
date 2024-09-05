
with

d365_source as (
    select *
    from {{ source("D365S", "inventitembarcode") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        itembarcode as itembarcode,
        itemid as itemid,
        inventdimid as inventdimid,
        barcodesetupid as barcodesetupid,
        useforprinting as useforprinting,
        useforinput as useforinput,
        description as description,
        qty as qty,
        unitid as unitid,
        retailvariantid as retailvariantid,
        retailshowforitem as retailshowforitem,
        blocked as blocked,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        null as del_modifiedtime,
        modifiedby as modifiedby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed

