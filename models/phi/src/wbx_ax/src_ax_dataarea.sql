with source as (

    select * from {{ source('WEETABIX', 'dataarea') }}

),

renamed as (

    select
        id,
        name,
        isvirtual,
        alwaysnative,
        timezone,
        recversion,
        partition,
        recid

    from source

)

select * from renamed