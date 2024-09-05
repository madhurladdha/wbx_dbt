

with source as (

    select * from {{ source('D365', 'wbxcust_trade_type') }} where _fivetran_deleted='FALSE'

),

renamed as (

    select
        recid,
        _sys_row_id,
        data_lake_modified_date_time,
        trade_type,
        description,
        upper(data_area_id) as data_area_id,
        partition,
        recversion,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        _fivetran_deleted,
        _fivetran_synced,
        last_processed_change_date_time

    from source

)

select * from renamed

