
   with d365_source as (
        select *
        from {{ source("D365", "wrk_ctr_activity_resource_requirement") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(resource_data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),

renamed as (


    select
        'D365' as source,
        activity_requirement as activityrequirement,
        upper(resource_data_area_id) as resourcedataareaid,
        wrk_ctr_id as wrkctrid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed