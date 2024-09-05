with source as (

    select * from {{ source('WEETABIX', 'purchtable') }}

),

renamed as (

    select
        purchid,
        purchname,
        orderaccount,
        invoiceaccount,
        freightzone,
        email,
        deliverydate,
        deliverytype,
        addressrefrecid,
        addressreftableid,
        intercompanyoriginalsalesid,
        intercompanyoriginalcustacct,
        currencycode,
        payment,
        cashdisc,
        countyorigdest,
        intercompanydirectdelivery,
        vendgroup,
        linedisc,
        discpercent,
        pricegroupid,
        multilinedisc,
        enddisc,
        intercocustpurchorderformnum,
        taxgroup,
        dlvterm,
        dlvmode,
        purchstatus,
        markupgroup,
        purchasetype,
        url,
        postingprofile,
        transactioncode,
        enterprisenumber,
        settlevoucher,
        intercoallowindirectcreation,
        intercompanyorigin,
        cashdiscpercent,
        deliveryname,
        covstatus,
        paymentsched,
        inventsiteid,
        onetimevendor,
        returnitemnum,
        freightsliptype,
        documentstatus,
        contactpersonid,
        inventlocationid,
        bankcentralbankpurposecode,
        itembuyergroupid,
        projid,
        purchpoolid,
        vatnum,
        port,
        incltax,
        bankcentralbankpurposetext,
        numbersequencegroup,
        languageid,
        autosummarymoduletype,
        transport,
        paymmode,
        paymspec,
        fixedduedate,
        statprocid,
        vendorref,
        intercompanycompanyid,
        intercompanysalesid,
        intercompanyorder,
        returnreasoncodeid,
        returnreplacementcreated,
        reqattention,
        defaultdimension,
        confirmeddlvearliest,
        contractnum_sa,
        changerequestrequired,
        reasontableref,
        documentstate,
        ismodified,
        matchingagreement,
        systementrysource,
        systementrychangepolicy,
        manualentrychangepolicy,
        vendinvoicedeclaration_is,
        workerpurchplacer,
        deliverypostaladdress,
        bankdocumenttype,
        listcode,
        isencumbrancerequired,
        sourcedocumentline,
        sourcedocumentheader,
        requester,
        accountingdistributiontemplate,
        accountingdate,
        finalizeclosingdate,
        constarget_jp,
        intrastatfulfillmentdate_hu,
        unitedvatinvoice_lt,
        intrastataddvalue_lv,
        invoiceregister_lt,
        packingslipregister_lt,
        servicename,
        serviceaddress,
        onetimesupplier,
        servicedate,
        confirmeddlv,
        fshautocreated,
        crossdockingdate,
        servicecategory,
        availsalesdate,
        localdeliverydate,
        replenishmentlocation,
        retailretailstatustype,
        retaildriverdetails,
        retailconcessionpurch,
        confirmingpo,
        mcrdropshipment,
        exchangeratedate,
        fixedexchrate,
        tamvendrebategroupid,
        taxperiodpaymentcode_pl,
        transportationdocument,
        createddatetime,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid,
        wbxinvoicehandling,
        bisedidelivery,
        bisediprocess

    from source

)

select * from renamed