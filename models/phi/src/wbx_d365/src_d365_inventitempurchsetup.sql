with
    d365_source as (
        select *
        from {{ source("D365", "invent_item_purch_setup") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 
    ),

    renamed as (


        select
            'D365' as source,
            item_id as itemid,
            invent_dim_id as inventdimid,
            invent_dim_id_default as inventdimiddefault,
            mandatory_invent_site as mandatoryinventsite,
            mandatory_invent_location as mandatoryinventlocation,
            multiple_qty as multipleqty,
            lowest_qty as lowestqty,
            highest_qty as highestqty,
            standard_qty as standardqty,
            lead_time as leadtime,
            calendar_days as calendardays,
            stopped as stopped,
            override as override,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed 