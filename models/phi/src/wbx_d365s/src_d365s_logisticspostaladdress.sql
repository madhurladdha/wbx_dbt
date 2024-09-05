with d365_source as (
    select *
    from {{ source("D365S", "logisticspostaladdress") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365S' as source,
        address as address,
        countryregionid as countryregionid,
        zipcode as zipcode,
        state as state,
        county as county,
        city as city,
        street as street,
        latitude as latitude,
        longitude as longitude,
        timezone as timezone,
        null as streetnumber,
        buildingcompliment as buildingcompliment,
        null as postbox,
        cityrecid as cityrecid,
        district as district,
        location as location,
        zipcoderecid as zipcoderecid,
        --validto and validfrom casted to TIMESTAMP_NTZ and respective tzid set to null
        cast(validto as TIMESTAMP_NTZ) as validto,
        null as validtotzid,
        cast(validfrom as TIMESTAMP_NTZ) as validfrom,
        null as validfromtzid,
        null as districtname,
        streetid_ru as streetid_ru,
        houseid_ru as houseid_ru,
        flatid_ru as flatid_ru,
        null as apartment_ru,
        null as building_ru,
        null as citykana_jp,
        isprivate as isprivate,
        privateforparty as privateforparty,
        null as streetkana_jp,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as biseancodeid
    from d365_source

)

select * from renamed
