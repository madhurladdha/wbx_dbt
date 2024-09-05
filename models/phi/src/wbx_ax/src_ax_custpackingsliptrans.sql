

with source as (

    select * from {{ source('WEETABIX', 'custpackingsliptrans') }}

),

renamed as (

    select
        packingslipid,
        deliverydate,
        linenum,
        inventtransid,
        itemid,
        externalitemid,
        name,
        ordered,
        qty,
        remain,
        salesgroup,
        priceunit,
        valuemst,
        salesid,
        salesunit,
        transactioncode,
        transport,
        countryregionofshipment,
        inventrefid,
        origsalesid,
        lineheader,
        inventdimid,
        statprocid,
        port,
        numbersequencegroup,
        intercompanyinventtransid,
        remaininvent,
        scrap,
        statvaluemst,
        intrastatdispatchid,
        inventqty,
        inventreftransid,
        deliverytype,
        inventreftype,
        salescategory,
        itemcodeid,
        origcountryregionid,
        origstateid,
        weight,
        deliverypostaladdress,
        defaultdimension,
        saleslineshippingdaterequested,
        saleslineshippingdateconfirmed,
        sourcedocumentline,
        fullymatched,
        stockedproduct,
        ngpcodestable_fr,
        invoicetransrefrecid,
        intrastatfulfillmentdate_hu,
        statisticvalue_lt,
        amountcur,
        currencycode,
        dlvterm,
        pdscwqty,
        pdscwremain,
        createddatetime,
        del_createdtime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
