with d365_source as (
    select *
    from {{ source("D365S", "unitofmeasure") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365S' as source,
        symbol as symbol,
        unitofmeasureclass as unitofmeasureclass,
        systemofunits as systemofunits,
        decimalprecision as decimalprecision,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
