

with source as (

    select * from {{ source('WEETABIX', 'inventsite') }}

),

renamed as (

    select
        siteid,
        name,
        defaultdimension,
        timezone,
        orderentrydeadlinegroupid,
        defaultinventstatusid,
        taxbranchrefrecid,
        isreceivingwarehouseovrdealwd,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
