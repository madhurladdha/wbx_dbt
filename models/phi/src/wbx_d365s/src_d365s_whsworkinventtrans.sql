with d365_source as (
    select *
    from {{ source("D365S", "whsworkinventtrans") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        workid as workid,
        linenum as linenum,
        inventtransidfrom as inventtransidfrom,
        inventtransidto as inventtransidto,
        itemid as itemid,
        inventdimidfrom as inventdimidfrom,
        inventdimidto as inventdimidto,
        inventtransidparent as inventtransidparent,
        qty as qty,
        inventqtyremain as inventqtyremain,
        workhasreservation as workhasreservation,
        cast(transdatetime as TIMESTAMP_NTZ) as transdatetime,
        --transdatetimetzid set to null
        null as transdatetimetzid,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
