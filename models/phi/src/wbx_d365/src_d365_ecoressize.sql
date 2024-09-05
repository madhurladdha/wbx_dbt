
with d365_source as (
    select *
    from {{ source("D365", "eco_res_size") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365' as source,
        name,
        recversion,
        partition as partition,
        recid as recid
    from d365_source
)

select *
from renamed