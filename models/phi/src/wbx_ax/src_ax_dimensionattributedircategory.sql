with
    source as (select * from {{ source("WEETABIX", "dimensionattributedircategory") }}),

    renamed as (

        select dimensionattribute, dircategory, recversion, partition, recid from source

    )

select *
from renamed
