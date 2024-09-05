

with source as (

    select *
    from {{ source('D365S', 'ecoresintvalue') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        recid,
        _fivetran_synced,
        /*_sys_row_id, last_processed_change_date_time,
        data_lake_modified_date_time set to null
        columns not found in WBX_D365S*/
        null as _sys_row_id,
        null as last_processed_change_date_time,
        null as data_lake_modified_date_time,
        intunitofmeasure as int_unit_of_measure,
        intvalue as int_value,
        _fivetran_deleted

    from source

)

select * from renamed

