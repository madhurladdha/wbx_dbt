

with source as (

    select * from {{ source('WEETABIX', 'fiscalcalendarperiod') }}

),

renamed as (

    select
        enddate,
        description,
        month,
        quarter,
        startdate,
        fiscalcalendaryear,
        shortname,
        name,
        type,
        fiscalcalendar,
        modifieddatetime,
        modifiedby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
