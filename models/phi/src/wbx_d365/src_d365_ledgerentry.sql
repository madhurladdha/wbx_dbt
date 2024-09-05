with
d365_source as (
    select *
    from {{ source("D365", "ledger_entry") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365' as source,
        general_journal_account_entry as generaljournalaccountentry,
        null as consolidatedcompany,
        payment_mode as paymentmode,
        null as thirdpartybankaccount,
        null as companybankaccount,
        is_bridging_posting as isbridgingposting,
        null as bankdataareaid,
        is_exchange_adjustment as isexchangeadjustment,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
