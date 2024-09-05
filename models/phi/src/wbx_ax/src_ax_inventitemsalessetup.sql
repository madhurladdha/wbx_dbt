

with source as (

    select * from {{ source('WEETABIX', 'inventitemsalessetup') }}

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
        atpinclplannedorders,
        stopped,
        override,
        atptimefence,
        deliverydatecontroltype,
        overridesalesleadtime,
        atpapplysupplytimefence,
        atpapplydemandtimefence,
        atpbackwarddemandtimefence,
        atpbackwardsupplytimefence,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
