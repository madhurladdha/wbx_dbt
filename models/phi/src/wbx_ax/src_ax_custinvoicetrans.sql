

with source as (

    select * from {{ source('WEETABIX', 'custinvoicetrans') }}

),

renamed as (

    select
        invoiceid,
        invoicedate,
        linenum,
        inventtransid,
        inventrefid,
        itemid,
        externalitemid,
        name,
        taxgroup,
        currencycode,
        priceunit,
        qty,
        salesprice,
        discpercent,
        discamount,
        lineamount,
        salesgroup,
        qtyphysical,
        partdelivery,
        remain,
        salesid,
        salesunit,
        salesmarkup,
        commisscalc,
        commissamountcur,
        transactioncode,
        deliverytype,
        countryregionofshipment,
        itemcodeid,
        taxautogenerated,
        taxitemgroup,
        taxamount,
        taxwritecode,
        multilndisc,
        multilnpercent,
        linedisc,
        linepercent,
        origsalesid,
        lineheader,
        transport,
        inventdimid,
        origcountryregionid,
        numbersequencegroup,
        statprocid,
        dlvdate,
        lineamounttax,
        port,
        assetid,
        customerlinenum,
        assetbookid,
        lineamountmst,
        taxamountmst,
        lineamounttaxmst,
        commissamountmst,
        sumlinedisc,
        sumlinediscmst,
        intercompanyinventtransid,
        olapcostvalue,
        einvoiceaccountcode,
        returndispositioncodeid,
        statlineamountmst,
        intrastatdispatchid,
        inventqty,
        origstate,
        inventreftype,
        inventreftransid,
        weight,
        returnarrivaldate,
        returncloseddate,
        reversechargeapplies_uk,
        reasonrefrecid,
        remainbefore,
        salescategory,
        custinvoicelineidref,
        sourcedocumentline,
        deliverypostaladdress,
        ledgerdimension,
        defaultdimension,
        taxwithholditemgroupheading_th,
        stockedproduct,
        reversedrecid,
        ngpcodestable_fr,
        parentrecid,
        taxwithholdgroup_th,
        intrastatfulfillmentdate_hu,
        billingcode,
        mcrdeliveryname,
        mcrdlvmode,
        pdscwqty,
        pdscwqtyphysical,
        pdscwremain,
        retailcategory,
        modifieddatetime,
        createddatetime,
        del_createdtime,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid,
        orderlinereference_no

    from source

)

select * from renamed
