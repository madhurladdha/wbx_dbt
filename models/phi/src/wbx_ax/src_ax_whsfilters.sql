with
    source as (select * from {{ source("WEETABIX", "whsfilters") }}),

    renamed as (

        select
            filternum,
            filtertitle,
            description,
            modifieddatetime,
            modifiedby,
            dataareaid,
            recversion,
            partition,
            recid

        from source

    )

select *
from renamed
