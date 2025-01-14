with source as (

    select * from {{ source('WEETABIX', 'salestable') }}

),

renamed as (

    select
        salesid,
        salesname,
        reservation,
        custaccount,
        invoiceaccount,
        deliverydate,
        url,
        purchorderformnum,
        salesgroup,
        freightsliptype,
        documentstatus,
        intercompanyoriginalsalesid,
        currencycode,
        payment,
        cashdisc,
        taxgroup,
        linedisc,
        custgroup,
        discpercent,
        intercooriginalcustaccount,
        pricegroupid,
        multilinedisc,
        enddisc,
        customerref,
        countyorigdest,
        listcode,
        dlvterm,
        dlvmode,
        purchid,
        salesstatus,
        markupgroup,
        salestype,
        salespoolid,
        postingprofile,
        transactioncode,
        intercompanyautocreateorders,
        intercompanydirectdelivery,
        intercompanydirectdeliveryorig,
        settlevoucher,
        enterprisenumber,
        intercoallowindirectcreation,
        intercoallowindirectcreatorig,
        deliveryname,
        onetimecustomer,
        covstatus,
        commissiongroup,
        paymentsched,
        intercompanyorigin,
        email,
        freightzone,
        returnitemnum,
        cashdiscpercent,
        contactpersonid,
        deadline,
        projid,
        inventlocationid,
        addressreftableid,
        vatnum,
        port,
        incltax,
        einvoicelinespec,
        numbersequencegroup,
        fixedexchrate,
        languageid,
        autosummarymoduletype,
        girotype,
        salesoriginid,
        estimate,
        transport,
        paymmode,
        paymspec,
        fixedduedate,
        exportreason,
        statprocid,
        bankcentralbankpurposetext,
        intercompanycompanyid,
        intercompanypurchid,
        intercompanyorder,
        dlvreason,
        quotationid,
        receiptdaterequested,
        receiptdateconfirmed,
        shippingdaterequested,
        shippingdateconfirmed,
        bankcentralbankpurposecode,
        einvoiceaccountcode,
        itemtagging,
        casetagging,
        pallettagging,
        addressrefrecid,
        custinvoiceid,
        inventsiteid,
        defaultdimension,
        creditcardcustrefid,
        shipcarrieraccount,
        shipcarrierid,
        shipcarrierfuelsurcharge,
        shipcarrierblindshipment,
        shipcarrierdeliverycontact,
        creditcardapprovalamount,
        creditcardauthorization,
        returndeadline,
        returnreplacementid,
        returnstatus,
        returnreasoncodeid,
        creditcardauthorizationerror,
        shipcarrieraccountcode,
        returnreplacementcreated,
        shipcarrierdlvtype,
        deliverydatecontroltype,
        shipcarrierexpeditedshipment,
        shipcarrierresidential,
        matchingagreement,
        systementrysource,
        systementrychangepolicy,
        manualentrychangepolicy,
        deliverypostaladdress,
        shipcarrierpostaladdress,
        shipcarriername,
        workersalestaker,
        sourcedocumentheader,
        bankdocumenttype,
        salesunitid,
        smmsalesamounttotal,
        smmcampaignid,
        customsexportorder_in,
        customsshippingbill_in,
        tdsgroup_in,
        tcsgroup_in,
        natureofassessee_in,
        constarget_jp,
        intrastatfulfillmentdate_hu,
        unitedvatinvoice_lt,
        intrastataddvalue_lv,
        invoiceregister_lt,
        packingslipregister_lt,
        bankaccount_lv,
        cashdiscbasedate,
        cashdiscbasedays,
        creditnotereasoncode,
        curbankaccount_lv,
        custbankaccount_lv,
        daxintegrationid,
        directdebitmandate,
        fiscaldoctype_pl,
        mcrorderstopped,
        pdsbatchattribautores,
        pdscustrebategroupid,
        pdsrebateprogramtmagroup,
        releasestatus,
        taxperiodpaymentcode_pl,
        transportationdocument,
        workersalesresponsible,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        modifiedtransactionid,
        createddatetime,
        del_createdtime,
        createdby,
        createdtransactionid,
        upper(trim(dataareaid)) as dataareaid,
        recversion,
        partition,
        recid,
        wbxaccountnumber,
        wbxcancelreasoncode,
        wbxcontractnumber,
        wbxcontractorindicator,
        wbxdeliveryinstruction,
        wbxfullpallet,
        wbxoneproductperpallet,
        wbxordercreationtype,
        wbxrequestedreceipttime,
        wbxsenddatetime,
        wbxsenddatetimetzid,
        wbxshelflife,
        wbxshelflifecheckneeded,
        wbxwaved,
        wbxpodrequired,
        wbxtotalsoqtydisc,
        wbxadditionaldisc,
        bisediconfirmation,
        bisedidelivery,
        bisediinvoice,
        bisedireturn,
        bisedipicking,
        retailchanneltable,
        ecommerceoperator_in,
        ecommerceoperatorgstin_in,
        ecommercesale_in,
        merchantid_in,
        provisionalassessment_in,
        allowzeropriceapprovedby,
        allowzeropriceapproveddate,
        allowzeroprices,
        pricecalculated,
        einvoicecfdiconfirmnumber_mx,
        satpaymmethod_mx,
        satpurpose_mx

    from source

)

select * from renamed
