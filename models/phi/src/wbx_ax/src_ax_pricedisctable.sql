with source as (

    select * from {{ source('WEETABIX', 'pricedisctable') }}

),

renamed as (

    select
        agreement,
        itemcode,
        accountcode,
        itemrelation,
        accountrelation,
        quantityamountfrom,
        fromdate,
        todate,
        amount,
        currency,
        percent1,
        percent2,
        deliverytime,
        searchagain,
        priceunit,
        relation,
        quantityamountto,
        unitid,
        markup,
        allocatemarkup,
        module,
        inventdimid,
        calendardays,
        genericcurrency,
        mcrpricediscgrouptype,
        mcrfixedamountcur,
        mcrmerchandisingeventid,
        agreementheaderext_ru,
        disregardleadtime,
        inventbaileefreedays_ru,
        maximumretailprice_in,
        originalpricediscadmtransrecid,
        pdscalculationid,
        modifieddatetime,
        dataareaid,
        recversion,
        partition,
        recid,
        wbxfixedexchangerate

    from source

)

select * from renamed