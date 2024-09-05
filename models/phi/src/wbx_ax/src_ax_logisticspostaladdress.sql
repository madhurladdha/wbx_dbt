

with source as (

    select * from {{ source('WEETABIX', 'logisticspostaladdress') }}

),

renamed as (

    select
        address,
        countryregionid,
        zipcode,
        state,
        county,
        city,
        street,
        latitude,
        longitude,
        timezone,
        streetnumber,
        buildingcompliment,
        postbox,
        cityrecid,
        district,
        location,
        zipcoderecid,
        validto,
        validtotzid,
        validfrom,
        validfromtzid,
        districtname,
        streetid_ru,
        houseid_ru,
        flatid_ru,
        apartment_ru,
        building_ru,
        citykana_jp,
        isprivate,
        privateforparty,
        streetkana_jp,
        modifieddatetime,
        modifiedby,
        recversion,
        partition,
        recid,
        biseancodeid

    from source

)

select * from renamed
