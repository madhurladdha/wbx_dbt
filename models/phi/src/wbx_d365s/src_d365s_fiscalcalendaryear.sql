with
d365_source as (
    select *
    from {{ source("D365S", "fiscalcalendaryear") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        cast(startdate as TIMESTAMP_NTZ) as startdate,
        cast(enddate as TIMESTAMP_NTZ) as enddate,
        fiscalcalendar as fiscalcalendar,
        name as name,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
