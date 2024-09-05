

with source as (

    select * from {{ source('WEETABIX', 'subledgervouchergeneraljournal') }}

),

renamed as (

    select
        voucher,
        voucherdataareaid,
        generaljournalentry,
        accountingdate,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
