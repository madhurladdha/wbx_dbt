with
d365_source as (
    select *
    from {{ source("D365S", "pricedisctable") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),


renamed as (


    select
        'D365S' as source,
        null as agreement,
        itemcode as itemcode,
        accountcode as accountcode,
        itemrelation as itemrelation,
        accountrelation as accountrelation,
        quantityamountfrom as quantityamountfrom,
        cast(fromdate as TIMESTAMP_NTZ) as fromdate,
        cast(todate as TIMESTAMP_NTZ) as todate,
        amount as amount,
        currency as currency,
        percent_1 as percent1,
        percent_2 as percent2,
        deliverytime as deliverytime,
        searchagain as searchagain,
        priceunit as priceunit,
        relation as relation,
        quantityamountto as quantityamountto,
        unitid as unitid,
        markup as markup,
        allocatemarkup as allocatemarkup,
        module as module,
        inventdimid as inventdimid,
        calendardays as calendardays,
        genericcurrency as genericcurrency,
        mcrpricediscgrouptype as mcrpricediscgrouptype,
        mcrfixedamountcur as mcrfixedamountcur,
        null as mcrmerchandisingeventid,
        agreementheaderext_ru as agreementheaderext_ru,
        disregardleadtime as disregardleadtime,
        inventbaileefreedays_ru as inventbaileefreedays_ru,
        maximumretailprice_in as maximumretailprice_in,
        originalpricediscadmtransrecid as originalpricediscadmtransrecid,
        null as pdscalculationid,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxfixedexchangerate

    from d365_source

)

select *
from renamed
