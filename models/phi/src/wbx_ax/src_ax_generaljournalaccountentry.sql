with source as (

    select * from {{ source('WEETABIX', 'generaljournalaccountentry') }}

),

renamed as (

    select
        transactioncurrencyamount,
        accountingcurrencyamount,
        reportingcurrencyamount,
        quantity,
        allocationlevel,
        iscorrection,
        iscredit,
        transactioncurrencycode,
        paymentreference,
        postingtype,
        ledgerdimension,
        generaljournalentry,
        text,
        reasonref,
        projid_sa,
        projtabledataareaid,
        historicalexchangeratedate,
        ledgeraccount,
        createdtransactionid,
        recversion,
        partition,
        recid,
        mainaccount

    from source

)

select * from renamed
