with
d365_source as (
    select *
    from {{ source("D365S", "ledgerentryjournal") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365S' as source,
        journalnumber as journalnumber,
        null as ledgerjournaltabledataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
