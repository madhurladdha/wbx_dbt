

with source as (

    select * from {{ source('WEETABIX', 'exchangeratetype') }}

),

renamed as (

    select
        name,
        description,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
