with
d365_source as (
    select *
    from {{ source("D365S", "logisticsaddressstate") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        name as name,
        stateid as stateid,
        countryregionid as countryregionid,
        null as intrastatcode,
        timezone as timezone,
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
