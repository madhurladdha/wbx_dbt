
with
d365_source as (
    select *
    from {{ source("D365S", "fintag") }} where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        recid as recid,
        /*_sys_row_id, lsn, lastprocessedchangedatetime,
        datalakemodifieddatetime set to null*/
        null as _sys_row_id,
        null as lsn,
        null as last_processed_change_date_time,
        null as data_lake_modified_date_time,
        tag_02 as tag_02,
        hash as hash,
        partition as partition,
        recversion,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        createdby as createdby,
        _fivetran_deleted as _fivetran_deleted,
        _fivetran_synced as _fivetran_synced,
        tag_01 as tag_01,
        tag_03 as tag_03
    from d365_source

)

select * from renamed