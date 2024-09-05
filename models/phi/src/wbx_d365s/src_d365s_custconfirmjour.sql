
with

d365_source as (
    select *
    from {{ source("D365S", "custconfirmjour") }}
    where
        upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
        and _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        confirmid as confirmid,
        cast(confirmdate as TIMESTAMP_NTZ) as confirmdate,
        salesid as salesid,
        orderaccount as orderaccount,
        invoiceaccount as invoiceaccount,
        custgroup as custgroup,
        purchaseorder as purchaseorder,
        deliveryname as deliveryname,
        dlvterm as dlvterm,
        dlvmode as dlvmode,
        payment as payment,
        null as cashdisccode,
        cashdiscpercent as cashdiscpercent,
        intercompanyposted as intercompanyposted,
        qty as qty,
        volume as volume,
        weight as weight,
        costvalue as costvalue,
        sumlinedisc as sumlinedisc,
        salesbalance as salesbalance,
        summarkup as summarkup,
        enddisc as enddisc,
        roundoff as roundoff,
        confirmamount as confirmamount,
        currencycode as currencycode,
        exchrate as exchrate,
        sumtax as sumtax,
        parmid as parmid,
        confirmdocnum as confirmdocnum,
        exchratesecondary as exchratesecondary,
        triangulation as triangulation,
        customerref as customerref,
        languageid as languageid,
        incltax as incltax,
        cast(fixedduedate as TIMESTAMP_NTZ) as fixedduedate,
        cast(deadline as TIMESTAMP_NTZ) as deadline,
        deliverypostaladdress as deliverypostaladdress,
        defaultdimension as defaultdimension,
        workersalestaker as workersalestaker,
        customsexportorder_in as customsexportorder_in,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as bisediprocess
    from d365_source

)

select * from renamed

