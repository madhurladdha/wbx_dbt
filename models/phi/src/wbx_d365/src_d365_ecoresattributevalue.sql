

with source as (

    select * from {{ source('D365', 'eco_res_attribute_value') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        recid,
        _sys_row_id,
        data_lake_modified_date_time,
        attribute,
        instance_value,
        value,
        partition,
        recversion,
        _fivetran_deleted,
        _fivetran_synced,
        last_processed_change_date_time

    from source

)

select * from renamed

