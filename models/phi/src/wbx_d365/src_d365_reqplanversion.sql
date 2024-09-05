with d365_source as (
        select *
        from {{ source("D365", "req_plan_version") }} where _FIVETRAN_DELETED='FALSE' AND upper(req_plan_data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),
renamed as (
 
    select 
        'D365' as source,
        active as active,
        upper(req_plan_data_area_id) as reqplandataareaid,
        req_plan_id as reqplanid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
)

select *  from renamed

