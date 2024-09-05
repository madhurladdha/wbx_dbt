

with source as (

    select * from {{ source('D365S', 'wbxcusttradesector') }} where _fivetran_deleted='FALSE'

),

renamed as (

    select
        recid,
        null as _sys_row_id,
        null as data_lake_modified_date_time,
        tradesector as trade_sector,
        description,
        upper(dataareaid) as data_area_id,
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
