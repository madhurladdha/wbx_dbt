

with source as (

    select * from {{ source('WEETABIX', 'unitofmeasure') }}

),

renamed as (

    select
        symbol,
        unitofmeasureclass,
        systemofunits,
        decimalprecision,
        modifieddatetime,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
