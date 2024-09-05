
with source as (

    select * from {{ source('D365', 'wbxcustomer_allowed_list') }} where _FIVETRAN_DELETED='FALSE'

),

renamed as (

    select
        recid,
        _fivetran_synced,
        _sys_row_id,
        data_lake_modified_date_time,
        customer_account as accountnum,
        item_number as itemid,
        variant_id,
        from_date as fromdate,
        to_date as todate,
        upper(data_area_id) as dataareaid,
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

