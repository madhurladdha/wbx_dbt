with
d365_source as (
    select *
    from {{ source("D365", "fiscal_calendar_year") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365' as source,
        start_date as startdate,
        end_date as enddate,
        fiscal_calendar as fiscalcalendar,
        cast(name as text(255)) as name,
        modifieddatetime as modifieddatetime,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
