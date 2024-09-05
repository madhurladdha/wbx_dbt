

with source as (

    select * from {{ source('WEETABIX', 'exchangerate') }}

),

renamed as (

    select
        exchangeratecurrencypair,
        exchangerate,
        validto,
        validfrom,
        modifieddatetime,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
