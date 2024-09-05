

with source as (

    select * from {{ source('D365', 'eco_res_attribute') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        recid,
        _sys_row_id,
        data_lake_modified_date_time,
        attribute_modifier,
        attribute_type,
        name,
        eng_chg_attribute_max,
        eng_chg_attribute_min,
        eng_chg_attribute_multiple,
        eng_chg_attribute_tolerance_action,
        partition,
        recversion,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed

