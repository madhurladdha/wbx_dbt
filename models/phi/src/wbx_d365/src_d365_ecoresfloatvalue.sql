

with source as (

    select * from {{ source('D365', 'eco_res_float_value') }} where _FIVETRAN_DELETED='FALSE'

),

renamed as (

    select
        recid,
        _fivetran_synced,
        _sys_row_id,
        last_processed_change_date_time,
        data_lake_modified_date_time,
        float_unit_of_measure,
        float_value,
        _fivetran_deleted

    from source

)

select * from renamed

