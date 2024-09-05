with d365_source as (
    select *
    from {{ source("D365", "dimension_attribute_value_combination") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365' as source,
        display_value as displayvalue,
        main_account as mainaccount,
        account_structure as accountstructure,
        ledger_dimension_type as ledgerdimensiontype,
        null as hash,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
)

select * from renamed
