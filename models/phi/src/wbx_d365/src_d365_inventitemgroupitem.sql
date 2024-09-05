with
    d365_source as (
        select *
        from {{ source("D365", "invent_item_group_item") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(item_data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 
    ),

    renamed as (

        select
            'D365' as source,
            item_id as itemid,
            upper(item_data_area_id) as itemdataareaid,
            item_group_id as itemgroupid,
            upper(item_group_data_area_id) as itemgroupdataareaid,
            recversion as recversion,
            partition  as partition,
            recid   as recid

        from d365_source

    )

select *
from renamed