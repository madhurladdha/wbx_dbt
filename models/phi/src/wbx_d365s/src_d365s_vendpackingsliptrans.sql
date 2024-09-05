with d365_source as (
    select *
    from {{ source("D365S", "vendpackingsliptrans") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (
    select
        'D365S' as source,
        packingslipid as packingslipid,
        cast(deliverydate as TIMESTAMP_NTZ) as deliverydate,
        linenum as linenum,
        inventtransid as inventtransid,
        destcountryregionid as destcountryregionid,
        itemid as itemid,
        null as externalitemid,
        name as name,
        ordered as ordered,
        qty as qty,
        remain as remain,
        priceunit as priceunit,
        valuemst as valuemst,
        inventrefid as inventrefid,
        inventreftype as inventreftype,
        purchunit as purchunit,
        null as transactioncode,
        inventreftransid as inventreftransid,
        intercompanyinventtransid as intercompanyinventtransid,
        deststate as deststate,
        remaininvent as remaininvent,
        origpurchid as origpurchid,
        returnactionid as returnactionid,
        null as transport,
        inventdimid as inventdimid,
        null as statprocid,
        null as port,
        cast(inventdate as TIMESTAMP_NTZ) as inventdate,
        null as numbersequencegroup,
        destcounty as destcounty,
        intrastatdispatchid as intrastatdispatchid,
        inventqty as inventqty,
        reasontableref as reasontableref,
        procurementcategory as procurementcategory,
        null as itemcodeid,
        null as origstateid,
        null as origcountryregionid,
        weight as weight,
        defaultdimension as defaultdimension,
        sourcedocumentline as sourcedocumentline,
        workerpurchaser as workerpurchaser,
        null as purchaselineexpecteddeldate,
        purchaselinelinenumber as purchaselinelinenumber,
        vendpackingslipjour as vendpackingslipjour,
        fullymatched as fullymatched,
        costledgervoucher as costledgervoucher,
        cast(accountingdate as TIMESTAMP_NTZ) as accountingdate,
        stockedproduct as stockedproduct,
        ngpcodestable_fr as ngpcodestable_fr,
        receivedqty_in as receivedqty_in,
        rejectedqty_in as rejectedqty_in,
        acceptedqty_in as acceptedqty_in,
        invoicetransrefrecid as invoicetransrefrecid,
        cast(intrastatfulfillmentdate_hu as TIMESTAMP_NTZ)
            as intrastatfulfillmentdate_hu,
        currencycode_w as currencycode_w,
        deviationqty_ru as deviationqty_ru,
        excisevalue_ru as excisevalue_ru,
        vatvalue_ru as vatvalue_ru,
        exciseamount_ru as exciseamount_ru,
        vatamount_ru as vatamount_ru,
        lineamount_w as lineamount_w,
        taxamount_ru as taxamount_ru,
        null as taxitemgroup_ru,
        null as taxgroup_ru,
        statisticvalue_lt as statisticvalue_lt,
        pdscwordered as pdscwordered,
        pdscwqty as pdscwqty,
        pdscwremain as pdscwremain,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
