with d365_source as (
    select *
    from {{ source("D365S", "dimensionattributevaluecombination") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365S' as source,
        displayvalue as displayvalue,
        mainaccount as mainaccount,
        accountstructure as accountstructure,
        ledgerdimensiontype as ledgerdimensiontype,
        null as hash,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
)

select * from renamed
