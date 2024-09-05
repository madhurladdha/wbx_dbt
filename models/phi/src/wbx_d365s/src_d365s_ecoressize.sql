
with d365_source as (
    select *
    from {{ source("D365S", "ecoressize") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365S' as source,
        name as name,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
)

select *
from renamed