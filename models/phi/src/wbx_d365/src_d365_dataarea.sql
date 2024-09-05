with
    d365_source as (
        select *
        from {{ source("D365", "data_area") }} where upper(trim(id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} and _FIVETRAN_DELETED='FALSE'
        
    ),

    renamed as (


        select
            'D365' as source,
            upper(id) as id,
            UPPER(name) as name,
            isvirtual as isvirtual,
            alwaysnative as alwaysnative,
            timezone as timezone,
            recversion as recversion,
            partition as partition,
            recid as recid
        from d365_source

    )

select *
from renamed 