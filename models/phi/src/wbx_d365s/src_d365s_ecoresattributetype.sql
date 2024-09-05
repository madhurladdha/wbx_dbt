

with source as (

    select *
    from {{ source('D365S', 'ecoresattributetype') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        recid as recid,
        --_sys_row_id, datalakemodifieddatetime set to null
        null as _sys_row_id,
        null as data_lake_modified_date_time,
        datatype as data_type,
        isenumeration as is_enumeration,
        ishidden as is_hidden,
        name as name,
        partition as partition,
        recversion as recversion,
        _fivetran_deleted as _fivetran_deleted,
        _fivetran_synced as _fivetran_synced

    from source

)

select * from renamed

