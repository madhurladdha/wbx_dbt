with
    d365_source as (
        select *
        from {{ source("D365", "hcm_position") }}  where _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (


        select
            'D365' as source,
            position_id as positionid,
            modifieddatetime,
            modifiedby,
            createddatetime,
            createdby,
            recversion,
            partition as  partition,
            recid as recid

        from d365_source --where upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    )

select * from renamed 
