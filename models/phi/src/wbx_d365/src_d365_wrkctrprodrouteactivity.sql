with d365_source as (
        select *
        from {{ source("D365", "wrk_ctr_prod_route_activity") }}
        where _FIVETRAN_DELETED='FALSE' and trim(upper(route_data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (


    select
        'D365' as source,
        activity as activity,
        upper(route_data_area_id) as routedataareaid,
        prod_id as prodid,
        opr_num as oprnum,
        opr_priority as oprpriority,
        recversion as recversion,
        partition as partition,
        recid as recid
        
    from d365_source

)

select * from renamed