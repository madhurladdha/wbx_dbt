

with source as (

    select * from {{ source('D365', 'wbxcust_trade_sector') }} where _fivetran_deleted='FALSE'

),

renamed as (

    select
        recid,
        _sys_row_id,
        data_lake_modified_date_time,
        trade_sector,
        description,
        upper(data_area_id) as data_area_id,
        partition,
        recversion,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed

