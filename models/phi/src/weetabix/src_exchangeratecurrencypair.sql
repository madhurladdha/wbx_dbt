

with source as (

    select * from {{ source('WEETABIX', 'exchangeratecurrencypair') }}

),

renamed as (

    select
        fromcurrencycode,
        tocurrencycode,
        exchangeratetype,
        exchangeratedisplayfactor,
        modifieddatetime,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
