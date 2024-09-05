with
d365_source as (
    select *
    from {{ source("D365", "logistics_address_state") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365' as source,
        name as name,
        state_id as stateid,
        country_region_id as countryregionid,
        null as intrastatcode,
        time_zone as timezone,
        properties_ru as properties_ru,
        null as gnislocation,
        null as ibgecode_br,
        null as statecode_it,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as statecode_in
    from d365_source

)

select *
from renamed
