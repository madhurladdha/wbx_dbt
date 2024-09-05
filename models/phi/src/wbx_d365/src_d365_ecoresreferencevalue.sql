

with source as (

    select * from {{ source('D365', 'eco_res_reference_value') }} where _FIVETRAN_DELETED='FALSE'
),

renamed as (

    select
        recid,
        _fivetran_synced,
        _sys_row_id,
        data_lake_modified_date_time,
        ref_field_id,
        ref_rec_id,
        ref_table_id,
        _fivetran_deleted

    from source

)

select * from renamed

