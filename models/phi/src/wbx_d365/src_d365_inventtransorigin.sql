with
    d365_source as (
        select *
        from {{ source("D365", "invent_trans_origin") }}  where _FIVETRAN_DELETED='FALSE' and  upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (


        select
            'D365' as source,
            invent_trans_id as inventtransid,
            reference_category as referencecategory,
            reference_id as referenceid,
            item_id as itemid,
            item_invent_dim_id as iteminventdimid,
            party as party,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            null as modifieddatetime,
            null as modifiedby,
            null as createddatetime,
            null as createdby

        from d365_source

    )

select * from renamed 