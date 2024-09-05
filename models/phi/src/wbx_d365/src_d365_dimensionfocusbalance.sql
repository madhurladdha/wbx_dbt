with
d365_source as (
    select *
    from {{ source("D365", "dimension_focus_balance") }}
    where _fivetran_deleted = 'FALSE'

),


renamed as (

    select
        'D365' as source,
        ledger,
        focus_ledger_dimension as focusledgerdimension,
        posting_layer as postinglayer,
        fiscal_calendar_period_type as fiscalcalendarperiodtype,
        accounting_date as accountingdate,
        is_system_generated_ultimo as issystemgeneratedultimo,
        focus_dimension_hierarchy as focusdimensionhierarchy,
        debit_accounting_currency_amount as debitaccountingcurrencyamount,
        credit_accounting_currency_amount as creditaccountingcurrencyamount,
        debit_reporting_currency_amount as debitreportingcurrencyamount,
        credit_reporting_currency_amount as creditreportingcurrencyamount,
        quantity,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
