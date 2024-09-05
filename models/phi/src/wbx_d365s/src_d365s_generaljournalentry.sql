with
d365_source as (
    select *
    from {{ source("D365S", "generaljournalentry") }}
    where
        _fivetran_deleted = 'FALSE'
        and trim(
            upper(subledgervoucherdataareaid)
        ) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        cast(accountingdate as TIMESTAMP_NTZ) as accountingdate,
        ledgerentryjournal as ledgerentryjournal,
        cast(acknowledgementdate as TIMESTAMP_NTZ) as acknowledgementdate,
        null as ledgerpostingjournal,
        fiscalcalendarperiod as fiscalcalendarperiod,
        postinglayer as postinglayer,
        ledger as ledger,
        null as ledgerpostingjournaldataareaid,
        journalnumber as journalnumber,
        transferid as transferid,
        budgetsourceledgerentryposted as budgetsourceledgerentryposted,
        fiscalcalendaryear as fiscalcalendaryear,
        subledgervoucher as subledgervoucher,
        trim(upper(subledgervoucherdataareaid)) as subledgervoucherdataareaid,
        cast(documentdate as TIMESTAMP_NTZ) as documentdate,
        documentnumber as documentnumber,
        journalcategory as journalcategory,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        createdby as createdby,
        createdtransactionid as createdtransactionid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed