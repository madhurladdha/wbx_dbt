with source as (

    select * from {{ source('WEETABIX', 'wrkctractivityresourcerqurmnt') }}

),

renamed as (

    select
        activityrequirement,
        resourcedataareaid,
        wrkctrid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed