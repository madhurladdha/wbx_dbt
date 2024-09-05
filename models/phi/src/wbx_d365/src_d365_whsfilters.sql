with d365_source as (
        select *
        from {{ source("D365", "whsfilters") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (

        select
            'D365' as source,
            filter_num as filternum,
            filter_title as filtertitle,
            description as description,
            modifieddatetime as modifieddatetime,
            modifiedby as modifiedby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select *
from renamed
