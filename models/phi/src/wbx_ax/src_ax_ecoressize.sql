

with source as (

    select * from {{ source('WEETABIX', 'ecoressize') }}

),

renamed as (

    select
        name,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
