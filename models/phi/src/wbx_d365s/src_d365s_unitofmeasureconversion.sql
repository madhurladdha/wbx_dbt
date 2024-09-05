with d365_source as (
    select *
    from {{ source("D365S", "unitofmeasureconversion") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365' as source,
        fromunitofmeasure as fromunitofmeasure,
        tounitofmeasure as tounitofmeasure,
        product as product,
        factor as factor,
        numerator as numerator,
        denominator as denominator,
        inneroffset as inneroffset,
        outeroffset as outeroffset,
        rounding as rounding,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed