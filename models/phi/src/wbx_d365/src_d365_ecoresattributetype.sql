

with source as (

    select *
    from {{ source('D365', 'eco_res_attribute_type') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        recid,
        _sys_row_id,
        data_lake_modified_date_time,
        data_type,
        is_enumeration,
        is_hidden,
        name,
        partition,
        recversion,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed

