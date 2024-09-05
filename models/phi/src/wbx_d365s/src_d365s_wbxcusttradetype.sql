with source as (

    select * from {{ source('D365S', 'wbxcusttradetype') }} where _fivetran_deleted='FALSE'

),

renamed as (

    select
        recid,
        null as _sys_row_id,
        null as data_lake_modified_date_time,
        tradetype as trade_type,
        description,
        upper(dataareaid) as data_area_id,
        partition,
        recversion,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        _fivetran_deleted,
        _fivetran_synced,
        null as last_processed_change_date_time

    from source

)

select * from renamed

