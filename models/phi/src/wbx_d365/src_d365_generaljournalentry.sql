with
    d365_source as (
        select *
        from {{ source("D365", "general_journal_entry") }} where _FIVETRAN_DELETED='FALSE' and trim(upper(subledger_voucher_data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),
renamed as (

    select 
        'D365' as source,
        accounting_date as accountingdate,
        ledger_entry_journal as ledgerentryjournal,
        acknowledgement_date as acknowledgementdate,
        null as ledgerpostingjournal,
        fiscal_calendar_period as fiscalcalendarperiod,
        posting_layer as postinglayer,
        ledger as ledger,
        null as ledgerpostingjournaldataareaid,
        journal_number as journalnumber,
        transfer_id as transferid,
        budget_source_ledger_entry_posted as budgetsourceledgerentryposted,
        fiscal_calendar_year as fiscalcalendaryear,
        subledger_voucher as subledgervoucher,
        trim(upper(subledger_voucher_data_area_id))   as subledgervoucherdataareaid,
        document_date as documentdate,
        document_number as documentnumber,
        journal_category as journalcategory,
        createddatetime as createddatetime,
        createdby as createdby,
        createdtransactionid as createdtransactionid,
        recversion as recversion,
        partition as partition,
        recid  as recid
    from d365_source

)

select * from renamed