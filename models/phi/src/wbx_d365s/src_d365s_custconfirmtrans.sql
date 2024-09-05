with

d365_source as (
    select *
    from {{ source("D365S", "custconfirmtrans") }}
    where
        trim(upper(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
        and _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        salesid as salesid,
        confirmid as confirmid,
        cast(confirmdate as TIMESTAMP_NTZ) as confirmdate,
        linenum as linenum,
        salescategory as salescategory,
        itemid as itemid,
        externalitemid as externalitemid,
        name as name,
        currencycode as currencycode,
        priceunit as priceunit,
        salesunit as salesunit,
        qty as qty,
        salesprice as salesprice,
        salesmarkup as salesmarkup,
        discpercent as discpercent,
        discamount as discamount,
        lineamount as lineamount,
        defaultdimension as defaultdimension,
        cast(dlvdate as TIMESTAMP_NTZ) as dlvdate,
        inventtransid as inventtransid,
        taxamount as taxamount,
        null as taxwritecode,
        multilndisc as multilndisc,
        multilnpercent as multilnpercent,
        linedisc as linedisc,
        linepercent as linepercent,
        taxgroup as taxgroup,
        taxitemgroup as taxitemgroup,
        null as salesgroup,
        origsalesid as origsalesid,
        lineheader as lineheader,
        inventdimid as inventdimid,
        inventqty as inventqty,
        lineamounttax as lineamounttax,
        stockedproduct as stockedproduct,
        dlvterm as dlvterm,
        pdscwqty as pdscwqty,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
