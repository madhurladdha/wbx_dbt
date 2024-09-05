

with source as (

    select * from {{ source('WEETABIX', 'custconfirmtrans') }}

),

renamed as (

    select
        salesid,
        confirmid,
        confirmdate,
        linenum,
        salescategory,
        itemid,
        externalitemid,
        name,
        currencycode,
        priceunit,
        salesunit,
        qty,
        salesprice,
        salesmarkup,
        discpercent,
        discamount,
        lineamount,
        defaultdimension,
        dlvdate,
        inventtransid,
        taxamount,
        taxwritecode,
        multilndisc,
        multilnpercent,
        linedisc,
        linepercent,
        taxgroup,
        taxitemgroup,
        salesgroup,
        origsalesid,
        lineheader,
        inventdimid,
        inventqty,
        lineamounttax,
        stockedproduct,
        dlvterm,
        pdscwqty,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
