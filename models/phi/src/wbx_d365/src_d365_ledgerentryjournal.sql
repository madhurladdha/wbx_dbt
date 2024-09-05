with
d365_source as (
    select *
    from {{ source("D365", "ledger_entry_journal") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365' as source,
        journal_number as journalnumber,
        null as ledgerjournaltabledataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
