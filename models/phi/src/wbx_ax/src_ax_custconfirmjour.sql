

with source as (

    select * from {{ source('WEETABIX', 'custconfirmjour') }}

),

renamed as (

    select
        confirmid,
        confirmdate,
        salesid,
        orderaccount,
        invoiceaccount,
        custgroup,
        purchaseorder,
        deliveryname,
        dlvterm,
        dlvmode,
        payment,
        cashdisccode,
        cashdiscpercent,
        intercompanyposted,
        qty,
        volume,
        weight,
        costvalue,
        sumlinedisc,
        salesbalance,
        summarkup,
        enddisc,
        roundoff,
        confirmamount,
        currencycode,
        exchrate,
        sumtax,
        parmid,
        confirmdocnum,
        exchratesecondary,
        triangulation,
        customerref,
        languageid,
        incltax,
        fixedduedate,
        deadline,
        deliverypostaladdress,
        defaultdimension,
        workersalestaker,
        customsexportorder_in,
        createddatetime,
        dataareaid,
        recversion,
        partition,
        recid,
        bisediprocess

    from source

)

select * from renamed
