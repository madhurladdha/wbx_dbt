

with source as (

    select * from {{ source('D365S', 'ecoresattributevalue') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        recid as recid,
        null as _sys_row_id,
        null as data_lake_modified_date_time,
        attribute as attribute,
        instancevalue as instance_value,
        value as value,
        partition as partition,
        recversion as recversion,
        _fivetran_deleted as _fivetran_deleted,
        _fivetran_synced as _fivetran_synced,
        null as last_processed_change_date_time

    from source

)

select * from renamed

