with
d365_source as (
    select *
    from {{ source("D365S", "fiscalcalendar") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        calendarid as calendarid,
        description,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby,
        recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed