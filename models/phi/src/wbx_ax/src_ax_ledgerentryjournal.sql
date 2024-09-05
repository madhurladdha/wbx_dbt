

with source as (

    select * from {{ source('WEETABIX', 'ledgerentryjournal') }}

),

renamed as (

    select
        journalnumber,
        ledgerjournaltabledataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
