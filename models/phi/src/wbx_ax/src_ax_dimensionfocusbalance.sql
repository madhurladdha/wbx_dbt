

with source as (

    select * from {{ source('WEETABIX', 'dimensionfocusbalance') }}

),

renamed as (

    select
        ledger,
        focusledgerdimension,
        postinglayer,
        fiscalcalendarperiodtype,
        accountingdate,
        issystemgeneratedultimo,
        focusdimensionhierarchy,
        debitaccountingcurrencyamount,
        creditaccountingcurrencyamount,
        debitreportingcurrencyamount,
        creditreportingcurrencyamount,
        quantity,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
