with
d365_source as (
    select *
    from {{ source("D365", "dimension_focus_ledger_dimension_reference") }}
    where _fivetran_deleted = 'FALSE'
),



renamed as (

    select
        'D365' as source,
        focus_ledger_dimension as focusledgerdimension,
        account_entry_ledger_dimension as accountentryledgerdimension,
        focus_dimension_hierarchy as focusdimensionhierarchy,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
