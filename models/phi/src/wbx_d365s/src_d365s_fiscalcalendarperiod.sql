with
d365_source as (
    select *
    from {{ source("D365S", "fiscalcalendarperiod") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        cast(enddate as TIMESTAMP_NTZ) as enddate,
        null as description,
        month as month,
        quarter as quarter,
        cast(startdate as TIMESTAMP_NTZ) as startdate,
        fiscalcalendaryear as fiscalcalendaryear,
        null as shortname,
        name as name,
        type as type,
        fiscalcalendar as fiscalcalendar,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
