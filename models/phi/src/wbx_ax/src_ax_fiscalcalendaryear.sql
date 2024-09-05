

with source as (

    select * from {{ source('WEETABIX', 'fiscalcalendaryear') }}

),

renamed as (

    select
        startdate,
        enddate,
        fiscalcalendar,
        name,
        modifieddatetime,
        modifiedby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
