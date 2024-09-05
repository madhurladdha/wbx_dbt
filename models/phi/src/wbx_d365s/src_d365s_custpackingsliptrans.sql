with
d365_source as (
    select *
    from {{ source("D365S", "custpackingsliptrans") }}
    where
        trim(upper(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
        and _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365S' as source,
        packingslipid as packingslipid,
        cast(deliverydate as TIMESTAMP_NTZ) as deliverydate,
        linenum as linenum,
        inventtransid as inventtransid,
        itemid as itemid,
        null as externalitemid,
        name as name,
        ordered as ordered,
        qty as qty,
        remain as remain,
        null as salesgroup,
        priceunit as priceunit,
        valuemst as valuemst,
        salesid as salesid,
        salesunit as salesunit,
        null as transactioncode,
        null as transport,
        countryregionofshipment as countryregionofshipment,
        inventrefid as inventrefid,
        origsalesid as origsalesid,
        lineheader as lineheader,
        inventdimid as inventdimid,
        null as statprocid,
        null as port,
        null as numbersequencegroup,
        null as intercompanyinventtransid,
        remaininvent as remaininvent,
        scrap as scrap,
        statvaluemst as statvaluemst,
        null as intrastatdispatchid,
        inventqty as inventqty,
        inventreftransid as inventreftransid,
        deliverytype as deliverytype,
        inventreftype as inventreftype,
        salescategory as salescategory,
        null as itemcodeid,
        null as origcountryregionid,
        null as origstateid,
        weight as weight,
        deliverypostaladdress as deliverypostaladdress,
        defaultdimension as defaultdimension,
        cast(saleslineshippingdaterequested as TIMESTAMP_NTZ)
            as saleslineshippingdaterequested,
        cast(saleslineshippingdateconfirmed as TIMESTAMP_NTZ)
            as saleslineshippingdateconfirmed,
        sourcedocumentline as sourcedocumentline,
        fullymatched as fullymatched,
        stockedproduct as stockedproduct,
        ngpcodestable_fr as ngpcodestable_fr,
        invoicetransrefrecid as invoicetransrefrecid,
        cast(intrastatfulfillmentdate_hu as TIMESTAMP_NTZ)
            as intrastatfulfillmentdate_hu,
        statisticvalue_lt as statisticvalue_lt,
        amountcur as amountcur,
        null as currencycode,
        dlvterm as dlvterm,
        pdscwqty as pdscwqty,
        pdscwremain as pdscwremain,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        null as del_createdtime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
