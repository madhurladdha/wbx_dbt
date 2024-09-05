with
d365_source as (
    select *
    from {{ source("D365S", "dimensionfocusbalance") }}
    where _fivetran_deleted = 'FALSE'

),


renamed as (

    select
        'D365S' as source,
        ledger,
        focusledgerdimension as focusledgerdimension,
        postinglayer as postinglayer,
        fiscalcalendarperiodtype as fiscalcalendarperiodtype,
        cast(accountingdate as TIMESTAMP_NTZ) as accountingdate,
        issystemgeneratedultimo as issystemgeneratedultimo,
        focusdimensionhierarchy as focusdimensionhierarchy,
        debitaccountingcurrencyamount as debitaccountingcurrencyamount,
        creditaccountingcurrencyamount as creditaccountingcurrencyamount,
        debitreportingcurrencyamount as debitreportingcurrencyamount,
        creditreportingcurrencyamount as creditreportingcurrencyamount,
        quantity,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
