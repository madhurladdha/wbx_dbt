

with source as (

    select * from {{ source('WEETABIX', 'vendinvoicetrans') }}

),

renamed as (

    select
        invoiceid,
        invoicedate,
        linenum,
        inventtransid,
        destcountryregionid,
        itemid,
        externalitemid,
        name,
        taxgroup,
        currencycode,
        priceunit,
        qty,
        purchprice,
        discpercent,
        discamount,
        lineamount,
        taxamount,
        qtyphysical,
        partdelivery,
        inventrefid,
        purchid,
        purchunit,
        purchmarkup,
        transactioncode,
        inventreftype,
        inventreftransid,
        deststate,
        taxwritecode,
        taxautogenerated,
        taxitemgroup,
        tax1099amount,
        tax1099date,
        tax1099num,
        multilndisc,
        multilnpercent,
        linedisc,
        linepercent,
        origpurchid,
        transport,
        internalinvoiceid,
        intercompanyinventtransid,
        numbersequencegroup,
        inventdimid,
        lineamounttax,
        port,
        statprocid,
        inventdate,
        destcounty,
        intrastatdispatchid,
        inventqty,
        tax1099state,
        tax1099stateamount,
        settletax1099amount,
        settletax1099stateamount,
        lineamountmst,
        reversechargeapplies_uk,
        tax1099recid,
        reasontableref,
        procurementcategory,
        itemcodeid,
        origstateid,
        origcountryregionid,
        weight,
        defaultdimension,
        ledgerdimension,
        tax1099fields,
        purchaselinelinenumber,
        sourcedocumentline,
        taxwithholditemgroupheading_th,
        linetype,
        description,
        deliveryname,
        deliverypostaladdress,
        operationtype_mx,
        stockedproduct,
        ngpcodestable_fr,
        advanceapplicationid,
        taxwithholdgroup_th,
        companylocation_in,
        customstariffcodetable_in,
        excisetariffcodes_in,
        registrationpostaladdress_in,
        salestaxformtypes_in,
        vendorlocation_in,
        excisetype_in,
        vatamount_in,
        vatdeferred_in,
        vatexpense_in,
        vatgoodstype_in,
        nonrecoverablepercent_in,
        tdsgroup_in,
        tcsgroup_in,
        gtaservicecategory_in,
        exciserecordtype_in,
        maximumretailprice_in,
        assessablevalue_in,
        creditnotedate_in,
        taxwithholdlinenum_in,
        taxwithholdvoucher_in,
        taxwithholdbasecur_th,
        intrastatfulfillmentdate_hu,
        excisevalue_ru,
        vatvalue_ru,
        exciseamount_ru,
        vatamount_ru,
        facturedqty_ru,
        facturedfully_ru,
        refreturninvoicetrans_w,
        statisticvalue_lt,
        vatdocumenttype_ru,
        lineamountmst_w,
        taxamountmst_w,
        vatamountmst_ru,
        exciseamountmst_ru,
        rbopackagelinenum,
        retailpackageid,
        agreementline_psn,
        alcohollicenseseriesnum_ru,
        countryregionname_ru,
        dsa_in,
        invoicegtdid_ru,
        ispwp,
        markupcode_ru,
        pdscwqty,
        pdscwqtyphysical,
        psareleaseamount,
        psaretainageamount,
        purchcommitmentline_psn,
        readyforpayment,
        servicecodetable_in,
        venddirective_psn,
        modifieddatetime,
        createddatetime,
        dataareaid,
        recversion,
        partition,
        recid,
        budgetreservationline_psn

    from source

)

select * from renamed
