with d365_source as (
    select *
    from {{ source("D365", "unit_of_measure_conversion") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365' as source,
        from_unit_of_measure as fromunitofmeasure,
        to_unit_of_measure as tounitofmeasure,
        product as product,
        factor as factor,
        numerator as numerator,
        denominator as denominator,
        inner_offset as inneroffset,
        outer_offset as outeroffset,
        rounding as rounding,
        modifieddatetime as modifieddatetime,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed