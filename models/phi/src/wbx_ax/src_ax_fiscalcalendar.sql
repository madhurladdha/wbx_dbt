

with source as (

    select * from {{ source('WEETABIX', 'fiscalcalendar') }}

),

renamed as (

    select
        calendarid,
        description,
        modifieddatetime,
        modifiedby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed

