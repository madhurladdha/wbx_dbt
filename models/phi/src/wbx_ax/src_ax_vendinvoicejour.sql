with source as (

    select * from {{ source('WEETABIX', 'vendinvoicejour') }}

),

renamed as (

    select
        vendgroup,
        purchid,
        orderaccount,
        invoiceaccount,
        invoiceid,
        invoicedate,
        duedate,
        cashdisc,
        cashdiscdate,
        qty,
        volume,
        weight,
        sumlinedisc,
        prepayment,
        salesbalance,
        enddisc,
        invoiceamount,
        currencycode,
        exchrate,
        enterprisenumber,
        returnitemnum,
        taxroundoff,
        ledgervoucher,
        taxprintoninvoice,
        taxspecifybyline,
        documentnum,
        documentdate,
        countryregionid,
        intrastatdispatch,
        invoiceroundoff,
        summarkup,
        paymid,
        taxgroup,
        cashdisccode,
        cashdiscpercent,
        payment,
        postingprofile,
        paymentsched,
        intercompanyposted,
        purchasetype,
        sumtax,
        parmid,
        exchratesecondary,
        triangulation,
        itembuyergroupid,
        vatnum,
        internalinvoiceid,
        numbersequencegroup,
        incltax,
        paymdayid,
        dlvterm,
        dlvmode,
        fixedduedate,
        intercompanycompanyid,
        intercompanysalesid,
        intercompanyledgervoucher,
        proforma,
        languageid,
        invoiceamountmst,
        summarkupmst,
        enddiscmst,
        reversecharge_uk,
        documentorigin,
        vendinvoicegroup,
        vendpaymentgroup,
        defaultdimension,
        description,
        contractnum_sa,
        remittanceaddress,
        vendinvoicedeclaration_is,
        costledgervoucher,
        sourcedocumentheader,
        sourcedocumentline,
        operationtype_mx,
        listcode,
        eusaleslist,
        vendorrequestedworkeremail,
        logisticselectronicaddress,
        banklcimportline,
        invoicetype,
        deliveryname,
        deliverydate_es,
        purchreceiptdate_w,
        vatonpayment_ru,
        correct_ru,
        whoisauthor_lt,
        invoicestatus_lt,
        inventprofiletype_ru,
        taxinvoicepurchid,
        numbersequencecode_lt,
        correctedinvoicedate_ru,
        vatamount_in,
        taxwithholdamount_in,
        vendconsinvoice_jp,
        deliveryname_lt,
        deliveryaddress_lt,
        intrastatfulfillmentdate_hu,
        unitedvatinvoice_lt,
        nonrealrevenue_ru,
        offsessionid_ru,
        consigneeaccount_ru,
        consignoraccount_ru,
        facturedfully_ru,
        attorneyissuedname_ru,
        attorneyid_ru,
        attorneydate_ru,
        stateinvoiceprinted_lv,
        dlvaddress_lv,
        intrastataddvalue_lv,
        invoiceregister_lt,
        purchagreementheader_psn,
        constarget_jp,
        correctedinvoiceid_ru,
        correctiontype_ru,
        deliverypostaladdress,
        fiscaldocumenttype_br,
        inventbaileereceiptreportid_ru,
        reasontableref_br,
        salespurchoperationtype_br,
        servicecodeondlvaddress_br,
        taxsetoffvoucher_in,
        transitcontrolvoucher_br,
        transportationdocument,
        vendfinaluser_br,
        modifieddatetime,
        createddatetime,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid,
        cfdiuuid_mx,
        invoiceseries_mx,
        invoiceidentification_in

    from source

)

select * from renamed
