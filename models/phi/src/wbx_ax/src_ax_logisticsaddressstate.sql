

with source as (

    select * from {{ source('WEETABIX', 'logisticsaddressstate') }}

),

renamed as (

    select
        name,
        stateid,
        countryregionid,
        intrastatcode,
        timezone,
        properties_ru,
        gnislocation,
        ibgecode_br,
        statecode_it,
        recversion,
        partition,
        recid,
        statecode_in

    from source

)

select * from renamed
