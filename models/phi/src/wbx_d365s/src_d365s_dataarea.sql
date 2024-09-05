with
d365_source as (
    select *
    from {{ source("D365S", "dataarea") }}
    where
        upper(trim(fno_id)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
        and _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365S' as source,
        --selects fno_id to avoid generic synapse generated id
        upper(fno_id) as id,
        upper(name) as name,
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