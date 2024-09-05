with source as (

    select * from {{ source('WEETABIX', 'accountingdistribution') }}

),

renamed as (

    select
        transactioncurrency,
        transactioncurrencyamount,
        ledgerdimension,
        amountsource,
        referencedistribution,
        accountingevent,
        sourcedocumentline,
        type,
        accountinglegalentity,
        parentdistribution,
        role,
        accountingdate,
        allocationfactor,
        monetaryamount,
        number_,
        referencerole,
        finalizeaccountingevent,
        sourcedocumentheader,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
