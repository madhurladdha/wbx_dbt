with
    d365_source as (
        select *
        from {{ source("D365", "forecast_item_allocation_line") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),

    renamed as (


        select
            'D365' as source,
            allocation_id as allocationid,
            line_num as linenum,
            item_id as itemid,
            percent_ as percent_,
            invent_dim_id as inventdimid,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source 

    )

select *
from renamed
