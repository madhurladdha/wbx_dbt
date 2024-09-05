with
d365_source as (
    select *
    from {{ source("D365", "general_journal_account_entry") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365' as source,
        transaction_currency_amount as transactioncurrencyamount,
        accounting_currency_amount as accountingcurrencyamount,
        reporting_currency_amount as reportingcurrencyamount,
        quantity as quantity,
        allocation_level as allocationlevel,
        is_correction as iscorrection,
        is_credit as iscredit,
        transaction_currency_code as transactioncurrencycode,
        null as paymentreference,
        posting_type as postingtype,
        ledger_dimension as ledgerdimension,
        general_journal_entry as generaljournalentry,
        text as text,
        reason_ref as reasonref,
        null as projid_sa,
        null as projtabledataareaid,
        historical_exchange_rate_date as historicalexchangeratedate,
        ledger_account as ledgeraccount,
        createdtransactionid as createdtransactionid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        main_account as mainaccount,
        fin_tag
    from d365_source

)

select * from renamed
