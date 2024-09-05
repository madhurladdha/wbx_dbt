with

d365_source as (
    select *
    from {{ source("D365S", "dimensionattributelevelvalue") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        ordinal,
        displayvalue as displayvalue,
        dimensionattributevalue as dimensionattributevalue,
        dimensionattributevaluegroup as dimensionattributevaluegroup,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed