
with

    d365_source as (
        select *
        from {{ source("D365", "invent_item_barcode") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 
    ),

    renamed as (

        select
            'D365' as source,
            item_bar_code as itembarcode,
            item_id as itemid,
            invent_dim_id as inventdimid,
            barcode_setup_id as barcodesetupid,
            use_for_printing as useforprinting,
            use_for_input as useforinput,
            description as description,
            qty as qty,
            unit_id as unitid,
            retail_variant_id as retailvariantid,
            retail_show_for_item as retailshowforitem,
            blocked as blocked,
            modifieddatetime as modifieddatetime,
            null as del_modifiedtime,
            modifiedby as modifiedby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select *
from renamed

