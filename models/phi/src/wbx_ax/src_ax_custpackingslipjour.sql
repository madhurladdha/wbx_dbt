

with source as (

    select * from {{ source('WEETABIX', 'custpackingslipjour') }}

),

renamed as (

    select
        refnum,
        salesid,
        orderaccount,
        invoiceaccount,
        packingslipid,
        purchaseorder,
        deliverydate,
        deliveryname,
        qty,
        volume,
        weight,
        printed,
        intercompanyposted,
        intrastatdispatch,
        dlvterm,
        dlvmode,
        printmgmtsiteid,
        ledgervoucher,
        returnitemnum,
        salestype,
        freightsliptype,
        freightslipnum,
        parmid,
        listcode,
        customerref,
        languageid,
        inventlocationid,
        billofladingid,
        exportreason,
        documentdate,
        numbersequencegroup,
        intercompanycompanyid,
        intercompanypurchid,
        bolpackageappearance,
        bolcarriername,
        boladdress,
        invoicingname,
        dlvreason,
        bolfreightedby,
        returnpackingslipid,
        shipcarrierdeliverycontact,
        shipcarrieraccount,
        shipcarrierid,
        shipcarrierblindshipment,
        shipcarrierphone,
        shipcarrieremail,
        deliverypostaladdress,
        invoicepostaladdress,
        defaultdimension,
        workersalestaker,
        sourcedocumentheader,
        internalpackingslipid,
        compiler,
        transportationdeliveryloader,
        transportationdeliveryowner,
        transportationdeliverycontractor,
        intrastatfulfillmentdate_hu,
        inventprofiletype_ru,
        packingslipregister_lt,
        packingslipnumberingcode_lt,
        packingslipstatus_lt,
        printblankdate_lt,
        contactpersonid,
        invoiceissueduedate_w,
        offsessionid_ru,
        pdscwqty,
        reasontableref_br,
        transportationdocument,
        banklcexportline,
        createddatetime,
        del_createdtime,
        dataareaid,
        recversion,
        partition,
        recid,
        bisediprocess

    from source

)

select * from renamed
