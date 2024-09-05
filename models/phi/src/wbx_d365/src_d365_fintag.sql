
with
d365_source as (
    select *
    from {{ source("D365", "fin_tag") }} where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        recid,
        _sys_row_id,
        lsn,
        last_processed_change_date_time,
        data_lake_modified_date_time,
        tag_02,
        hash,
        partition,
        recversion,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        _fivetran_deleted,
        _fivetran_synced,
        tag_01,
        tag_03
    from d365_source

)

select * from renamed