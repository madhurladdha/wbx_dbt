

with source as (

    select * from {{ source('D365S', 'ecoresattribute') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        recid,
        --_sys_row_id and datalakemodifieddatetime set to null
        null as _sys_row_id,
        null as data_lake_modified_date_time,
        attributemodifier as attribute_modifier,
        attributetype as attribute_type,
        name as name,
        engchgattributemax as eng_chg_attribute_max,
        engchgattributemin as eng_chg_attribute_min,
        engchgattributemultiple as eng_chg_attribute_multiple,
        engchgattributetoleranceaction as eng_chg_attribute_tolerance_action,
        partition as partition,
        recversion as recversion,
        _fivetran_deleted as _fivetran_deleted,
        _fivetran_synced as _fivetran_synced

    from source

)

select * from renamed

