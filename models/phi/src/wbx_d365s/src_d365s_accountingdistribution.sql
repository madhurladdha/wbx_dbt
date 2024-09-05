with d365_source as (
    select *
    from {{ source("D365S", "accountingdistribution") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        transactioncurrency as transactioncurrency,
        transactioncurrencyamount as transactioncurrencyamount,
        ledgerdimension as ledgerdimension,
        amountsource as amountsource,
        referencedistribution as referencedistribution,
        accountingevent as accountingevent,
        sourcedocumentline as sourcedocumentline,
        type as type,
        accountinglegalentity as accountinglegalentity,
        parentdistribution as parentdistribution,
        null as role,
        cast(accountingdate as TIMESTAMP_NTZ) as accountingdate,
        allocationfactor as allocationfactor,
        monetaryamount as monetaryamount,
        number as number_,
        referencerole as referencerole,
        finalizeaccountingevent as finalizeaccountingevent,
        sourcedocumentheader as sourcedocumentheader,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
