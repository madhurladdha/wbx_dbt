
with source as (

    select * from {{ source('D365S', 'wbxcustomerallowedlist') }} where _FIVETRAN_DELETED='FALSE'

),

renamed as (

    select
        recid,
        _fivetran_synced,
        null as _sys_row_id,
        null as data_lake_modified_date_time,
        customeraccount as accountnum,
        itemnumber as itemid,
        variantid as variant_id,
        fromdate as fromdate,
        todate as todate,
        upper(dataareaid) as dataareaid,
        partition,
        recversion,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        _fivetran_deleted

    from source

)

select * from renamed

