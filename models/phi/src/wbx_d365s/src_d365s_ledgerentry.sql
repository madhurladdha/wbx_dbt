with
d365_source as (
    select *
    from {{ source("D365S", "ledgerentry") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        generaljournalaccountentry as generaljournalaccountentry,
        null as consolidatedcompany,
        paymentmode as paymentmode,
        null as thirdpartybankaccount,
        null as companybankaccount,
        isbridgingposting as isbridgingposting,
        null as bankdataareaid,
        isexchangeadjustment as isexchangeadjustment,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
