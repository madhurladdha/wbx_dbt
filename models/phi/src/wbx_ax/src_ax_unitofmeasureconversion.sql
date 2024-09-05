

with source as (

    select * from {{ source('WEETABIX', 'unitofmeasureconversion') }}

),

renamed as (

    select
        fromunitofmeasure,
        tounitofmeasure,
        product,
        factor,
        numerator,
        denominator,
        inneroffset,
        outeroffset,
        rounding,
        modifieddatetime,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
