with
    d365_source as (
        select *
        from {{ source("D365", "invent_model_group_item") }} where _FIVETRAN_DELETED='FALSE' and upper(item_data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (


        select
            'D365' as source,
            upper(item_data_area_id) as itemdataareaid,
            model_group_id as modelgroupid,
            item_id as itemid,
            upper(model_group_data_area_id) as modelgroupdataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source 

    )

select *
from renamed
