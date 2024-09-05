with d365_source as (
    select *
    from {{ source("D365", "unit_of_measure") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365' as source,
        symbol as symbol,
        unit_of_measure_class as unitofmeasureclass,
        system_of_units as systemofunits,
        decimal_precision as decimalprecision,
        modifieddatetime as modifieddatetime,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
