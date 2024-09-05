with

d365_source as (
    select *
    from {{ source("D365S", "dimensionattributevaluesetitem") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        dimensionattributevalueset as dimensionattributevalueset,
        dimensionattributevalue as dimensionattributevalue,
        displayvalue as displayvalue,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed