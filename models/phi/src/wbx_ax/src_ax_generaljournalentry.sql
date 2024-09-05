with source as (

    select * from {{ source('WEETABIX', 'generaljournalentry') }}

),

renamed as (

    select
        accountingdate,
        ledgerentryjournal,
        acknowledgementdate,
        ledgerpostingjournal,
        fiscalcalendarperiod,
        postinglayer,
        ledger,
        trim(upper(ledgerpostingjournaldataareaid)) ledgerpostingjournaldataareaid,
        journalnumber,
        transferid,
        budgetsourceledgerentryposted,
        fiscalcalendaryear,
        subledgervoucher,
        trim(upper(subledgervoucherdataareaid)) as subledgervoucherdataareaid,
        documentdate,
        documentnumber,
        journalcategory,
        createddatetime,
        createdby,
        createdtransactionid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
