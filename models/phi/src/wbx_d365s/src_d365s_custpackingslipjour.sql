
with d365_source as (
    select *
    from {{ source("D365S", "custpackingslipjour") }}
    where
        trim(upper(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
        and _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365S' as source,
        refnum as refnum,
        salesid as salesid,
        orderaccount as orderaccount,
        invoiceaccount as invoiceaccount,
        packingslipid as packingslipid,
        purchaseorder as purchaseorder,
        cast(deliverydate as TIMESTAMP_NTZ) as deliverydate,
        deliveryname as deliveryname,
        qty as qty,
        volume as volume,
        weight as weight,
        printed as printed,
        intercompanyposted as intercompanyposted,
        null as intrastatdispatch,
        dlvterm as dlvterm,
        dlvmode as dlvmode,
        printmgmtsiteid as printmgmtsiteid,
        ledgervoucher as ledgervoucher,
        returnitemnum as returnitemnum,
        salestype as salestype,
        freightsliptype as freightsliptype,
        null as freightslipnum,
        parmid as parmid,
        listcode as listcode,
        customerref as customerref,
        languageid as languageid,
        inventlocationid as inventlocationid,
        null as billofladingid,
        null as exportreason,
        cast(documentdate as TIMESTAMP_NTZ) as documentdate,
        null as numbersequencegroup,
        null as intercompanycompanyid,
        null as intercompanypurchid,
        null as bolpackageappearance,
        null as bolcarriername,
        null as boladdress,
        invoicingname as invoicingname,
        null as dlvreason,
        bolfreightedby as bolfreightedby,
        returnpackingslipid as returnpackingslipid,
        null as shipcarrierdeliverycontact,
        null as shipcarrieraccount,
        null as shipcarrierid,
        shipcarrierblindshipment as shipcarrierblindshipment,
        null as shipcarrierphone,
        null as shipcarrieremail,
        deliverypostaladdress as deliverypostaladdress,
        invoicepostaladdress as invoicepostaladdress,
        defaultdimension as defaultdimension,
        workersalestaker as workersalestaker,
        sourcedocumentheader as sourcedocumentheader,
        internalpackingslipid as internalpackingslipid,
        compiler as compiler,
        transportationdeliveryloader as transportationdeliveryloader,
        transportationdeliveryowner as transportationdeliveryowner,
        transportationdeliverycontractor as transportationdeliverycontractor,
        cast(intrastatfulfillmentdate_hu as TIMESTAMP_NTZ)
            as intrastatfulfillmentdate_hu,
        inventprofiletype_ru as inventprofiletype_ru,
        null as packingslipregister_lt,
        null as packingslipnumberingcode_lt,
        null as packingslipstatus_lt,
        printblankdate_lt as printblankdate_lt,
        null as contactpersonid,
        cast(invoiceissueduedate_w as TIMESTAMP_NTZ) as invoiceissueduedate_w,
        null as offsessionid_ru,
        pdscwqty as pdscwqty,
        reasontableref_br as reasontableref_br,
        transportationdocument as transportationdocument,
        banklcexportline as banklcexportline,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        null as del_createdtime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as bisediprocess
    from d365_source

)

select * from renamed

