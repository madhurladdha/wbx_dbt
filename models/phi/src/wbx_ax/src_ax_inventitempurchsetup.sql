

with source as (

    select * from {{ source('WEETABIX', 'inventitempurchsetup') }}

),

renamed as (

    select
        itemid,
        inventdimid,
        inventdimiddefault,
        mandatoryinventsite,
        mandatoryinventlocation,
        multipleqty,
        lowestqty,
        highestqty,
        standardqty,
        leadtime,
        calendardays,
        stopped,
        override,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
