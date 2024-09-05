with
d365_source as (
    select *
    from {{ source("D365S", "dimensionattributevaluegroupcombination") }}
    where _fivetran_deleted = 'FALSE'


),

renamed as (


    select
        'D365S' as source,
        ordinal,
        dimensionattributevaluecombination as dimensionattributevaluecombo,
        dimensionattributevaluegroup as dimensionattributevaluegroup,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed