with source as (

    select * from {{ source('WEETABIX', 'vendpackingsliptrans') }}

),

renamed as (

    select
        packingslipid,
        deliverydate,
        linenum,
        inventtransid,
        destcountryregionid,
        itemid,
        externalitemid,
        name,
        ordered,
        qty,
        remain,
        priceunit,
        valuemst,
        inventrefid,
        inventreftype,
        purchunit,
        transactioncode,
        inventreftransid,
        intercompanyinventtransid,
        deststate,
        remaininvent,
        origpurchid,
        returnactionid,
        transport,
        inventdimid,
        statprocid,
        port,
        inventdate,
        numbersequencegroup,
        destcounty,
        intrastatdispatchid,
        inventqty,
        reasontableref,
        procurementcategory,
        itemcodeid,
        origstateid,
        origcountryregionid,
        weight,
        defaultdimension,
        sourcedocumentline,
        workerpurchaser,
        purchaselineexpecteddeldate,
        purchaselinelinenumber,
        vendpackingslipjour,
        fullymatched,
        costledgervoucher,
        accountingdate,
        stockedproduct,
        ngpcodestable_fr,
        receivedqty_in,
        rejectedqty_in,
        acceptedqty_in,
        invoicetransrefrecid,
        intrastatfulfillmentdate_hu,
        currencycode_w,
        deviationqty_ru,
        excisevalue_ru,
        vatvalue_ru,
        exciseamount_ru,
        vatamount_ru,
        lineamount_w,
        taxamount_ru,
        taxitemgroup_ru,
        taxgroup_ru,
        statisticvalue_lt,
        pdscwordered,
        pdscwqty,
        pdscwremain,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
