with d365_source as (
        select *
        from {{ source("D365", "whsphys_dim_uom") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),

    renamed as (
select
            'D365' as source,
            uom,
            item_id as itemid,
            NULL as physdimid,
            depth,
            height,
            weight,
            width,
            modifieddatetime,
            modifiedby,
            upper(data_area_id) as dataareaid,
            recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed