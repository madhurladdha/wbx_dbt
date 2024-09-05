with
d365_source as (
    select *
    from {{ source("D365", "fiscal_calendar_period") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        end_date as enddate,
        null as description,
        month as month,
        quarter as quarter,
        start_date as startdate,
        fiscal_calendar_year as fiscalcalendaryear,
        null as shortname,
        name as name,
        type as type,
        fiscal_calendar as fiscalcalendar,
        modifieddatetime as modifieddatetime,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
