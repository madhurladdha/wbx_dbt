

with source as (

    select * from {{ source('WEETABIX', 'ledgerentry') }}

),

renamed as (

    select
        generaljournalaccountentry,
        consolidatedcompany,
        paymentmode,
        thirdpartybankaccount,
        companybankaccount,
        isbridgingposting,
        bankdataareaid,
        isexchangeadjustment,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
