with d365_source as (
    select *
    from {{ source("D365", "accounting_distribution") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        transaction_currency as transactioncurrency,
        transaction_currency_amount as transactioncurrencyamount,
        ledger_dimension as ledgerdimension,
        amount_source as amountsource,
        reference_distribution as referencedistribution,
        accounting_event as accountingevent,
        source_document_line as sourcedocumentline,
        type as type,
        accounting_legal_entity as accountinglegalentity,
        parent_distribution as parentdistribution,
        null as role,
        accounting_date as accountingdate,
        allocation_factor as allocationfactor,
        monetary_amount as monetaryamount,
        number_ as number_,
        reference_role as referencerole,
        finalize_accounting_event as finalizeaccountingevent,
        source_document_header as sourcedocumentheader,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
