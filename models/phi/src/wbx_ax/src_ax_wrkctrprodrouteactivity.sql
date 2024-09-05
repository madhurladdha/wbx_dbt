with source as (

    select * from {{ source('WEETABIX', 'wrkctrprodrouteactivity') }}

),

renamed as (

    select
        activity,
        routedataareaid,
        prodid,
        oprnum,
        oprpriority,
        recversion,
        partition,
        recid

    from source

)

select * from renamed