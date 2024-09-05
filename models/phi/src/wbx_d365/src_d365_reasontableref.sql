with d365_source as (
        select *
        from {{ source("D365", "reason_table_ref") }} where _FIVETRAN_DELETED='FALSE' AND upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
            ),
    renamed as (
        select 
            'D365' as source,
            reason as reason,
            reason_comment as reasoncomment,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source

    )

select * from renamed
