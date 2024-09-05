with
d365_source as (
    select *
    from {{ source("D365S", "inventtablemodule") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (

    select
        'D365S' as source,
        itemid as itemid,
        moduletype as moduletype,
        unitid as unitid,
        price as price,
        priceunit as priceunit,
        markup as markup,
        null as linedisc,
        null as multilinedisc,
        enddisc as enddisc,
        taxitemgroupid as taxitemgroupid,
        markupgroupid as markupgroupid,
        cast(pricedate as TIMESTAMP_NTZ) as pricedate,
        priceqty as priceqty,
        allocatemarkup as allocatemarkup,
        overdeliverypct as overdeliverypct,
        underdeliverypct as underdeliverypct,
        null as suppitemgroupid,
        intercompanyblocked as intercompanyblocked,
        taxwithholditemgroupheading_th as taxwithholditemgroupheading_th,
        taxwithholdcalculate_th as taxwithholdcalculate_th,
        maximumretailprice_in as maximumretailprice_in,
        priceseccur_ru as priceseccur_ru,
        markupseccur_ru as markupseccur_ru,
        pdspricingprecision as pdspricingprecision,
        taxgstreliefcategory_my as taxgstreliefcategory_my,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        null as del_modifiedtime,
        modifiedby as modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        null as del_createdtime,
        createdby as createdby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
