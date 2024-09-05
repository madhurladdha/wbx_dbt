with
    source as (select * from {{ source("WEETABIX", "reasontableref") }}),

    renamed as (

        select reason, reasoncomment, dataareaid, recversion, partition, recid

        from source

    )

select *
from renamed
