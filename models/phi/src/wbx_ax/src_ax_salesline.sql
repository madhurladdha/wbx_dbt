

with source as (

    select * from {{ source('WEETABIX', 'salesline') }}

),

renamed as (

    select
        salesid,
        linenum,
        itemid,
        salesstatus,
        name,
        externalitemid,
        taxgroup,
        qtyordered,
        salesdelivernow,
        remainsalesphysical,
        remainsalesfinancial,
        costprice,
        salesprice,
        currencycode,
        linepercent,
        linedisc,
        lineamount,
        confirmeddlv,
        reservation,
        salesgroup,
        salesunit,
        priceunit,
        projtransid,
        inventtransid,
        custgroup,
        custaccount,
        salesqty,
        salesmarkup,
        inventdelivernow,
        multilndisc,
        multilnpercent,
        salestype,
        blocked,
        complete,
        remaininventphysical,
        transactioncode,
        countyorigdest,
        taxitemgroup,
        taxautogenerated,
        underdeliverypct,
        overdeliverypct,
        barcode,
        barcodetype,
        inventreftransid,
        inventreftype,
        inventrefid,
        intercompanyorigin,
        itembomid,
        itemrouteid,
        lineheader,
        scrap,
        dlvmode,
        inventtransidreturn,
        projcategoryid,
        projid,
        inventdimid,
        transport,
        statprocid,
        port,
        projlinepropertyid,
        receiptdaterequested,
        customerlinenum,
        packingunitqty,
        packingunit,
        intercompanyinventtransid,
        remaininventfinancial,
        deliveryname,
        deliverytype,
        customerref,
        purchorderformnum,
        receiptdateconfirmed,
        stattriangulardeal,
        shippingdaterequested,
        shippingdateconfirmed,
        addressrefrecid,
        addressreftableid,
        serviceorderid,
        itemtagging,
        casetagging,
        pallettagging,
        linedeliverytype,
        einvoiceaccountcode,
        shipcarrierid,
        shipcarrieraccount,
        shipcarrierdlvtype,
        shipcarrieraccountcode,
        salescategory,
        deliverydatecontroltype,
        activitynumber,
        ledgerdimension,
        returnallowreservation,
        matchingagreementline,
        systementrysource,
        systementrychangepolicy,
        manualentrychangepolicy,
        itemreplaced,
        returndeadline,
        expectedretqty,
        returnstatus,
        returnarrivaldate,
        returncloseddate,
        returndispositioncodeid,
        deliverypostaladdress,
        shipcarrierpostaladdress,
        shipcarriername,
        defaultdimension,
        sourcedocumentline,
        taxwithholditemgroupheading_th,
        stockedproduct,
        customsname_mx,
        customsdocnumber_mx,
        customsdocdate_mx,
        propertynumber_mx,
        itempbaid,
        refreturninvoicetrans_w,
        postingprofile_ru,
        taxwithholdgroup,
        intrastatfulfillmentdate_hu,
        assetid_ru,
        statisticvalue_lt,
        creditnoteinternalref_pl,
        psaprojproposalqty,
        psaprojproposalinventqty,
        pdsexcludefromrebate,
        retailvariantid,
        agreementskipautolink,
        countryregionname_ru,
        creditnotereasoncode,
        deliverytaxgroup_br,
        deliverytaxitemgroup_br,
        dlvterm,
        invoicegtdid_ru,
        mcrorderline2pricehistoryref,
        pdsbatchattribautores,
        pdscwexpectedretqty,
        pdscwinventdelivernow,
        pdscwqty,
        pdscwremaininventfinancial,
        pdscwremaininventphysical,
        pdsitemrebategroupid,
        pdssamelot,
        pdssamelotoverride,
        priceagreementdate_ru,
        psacontractlinenum,
        retailblockqty,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        createddatetime,
        del_createdtime,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid,
        wbxshelflife,
        wbxvariantmemo,
        orderlinereference_no,
        satunitcode_mx,
        satproductcode_mx

    from source

)

select * from renamed
