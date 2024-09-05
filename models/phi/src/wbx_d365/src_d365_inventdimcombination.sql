with
    d365_source as (
        select *
        from {{ source("D365", "invent_dim_combination") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}} 
    ),

    renamed as (

        select
            'D365' as source,
            item_id as itemid,
            invent_dim_id as inventdimid,
            distinct_product_variant  as distinctproductvariant,
            retail_variant_id as retailvariantid,
            createddatetime as createddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source

    )

select * from renamed

