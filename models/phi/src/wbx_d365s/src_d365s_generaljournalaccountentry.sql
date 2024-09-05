with
d365source as (
    select *
    from {{ source("D365S", "generaljournalaccountentry") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        transactioncurrencyamount as transactioncurrencyamount,
        accountingcurrencyamount as accountingcurrencyamount,
        reportingcurrencyamount as reportingcurrencyamount,
        quantity as quantity,
        allocationlevel as allocationlevel,
        iscorrection as iscorrection,
        iscredit as iscredit,
        transactioncurrencycode as transactioncurrencycode,
        null as paymentreference,
        postingtype as postingtype,
        ledgerdimension as ledgerdimension,
        generaljournalentry as generaljournalentry,
        text as text,
        reasonref as reasonref,
        null as projid_sa,
        null as projtabledataareaid,
        cast(historicalexchangeratedate as TIMESTAMP_NTZ)
            as historicalexchangeratedate,
        ledgeraccount as ledgeraccount,
        createdtransactionid as createdtransactionid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        mainaccount as mainaccount,
        fintag as fin_tag
    from d365source

)

select * from renamed
