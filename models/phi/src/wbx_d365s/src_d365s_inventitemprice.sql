with
d365_source as (
    select *
    from {{ source("D365S", "inventitemprice") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (
    select
        'D365S' as source,
        itemid as itemid,
        cast(versionid as varchar(255)) as versionid,
        pricetype as pricetype,
        inventdimid as inventdimid,
        markup as markup,
        priceunit as priceunit,
        cast(price as number(32, 16)) as price,
        pricecalcid as pricecalcid,
        unitid as unitid,
        priceallocatemarkup as priceallocatemarkup,
        priceqty as priceqty,
        cast(stdcosttransdate as timestamp_ntz) as stdcosttransdate,
        stdcostvoucher as stdcostvoucher,
        costingtype as costingtype,
        cast(activationdate as timestamp_ntz) as activationdate,
        priceseccur_ru as priceseccur_ru,
        markupseccur_ru as markupseccur_ru,
        cast(modifieddatetime as timestamp_ntz) as modifieddatetime,
        cast(createddatetime as timestamp_ntz) as createddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed
