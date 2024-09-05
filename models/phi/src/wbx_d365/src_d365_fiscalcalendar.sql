with
d365_source as (
    select *
    from {{ source("D365", "fiscal_calendar") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        calendar_id as calendarid,
        description,
        modifieddatetime,
        modifiedby,
        recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed