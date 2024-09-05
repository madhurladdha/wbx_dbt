with d365_source as (
    select *
    from {{ source("D365", "logistics_postal_address") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365' as source,
        address as address,
        country_region_id as countryregionid,
        zip_code as zipcode,
        state as state,
        county as county,
        city as city,
        street as street,
        latitude as latitude,
        longitude as longitude,
        time_zone as timezone,
        null as streetnumber,
        building_compliment as buildingcompliment,
        null as postbox,
        city_rec_id as cityrecid,
        district as district,
        location as location,
        zip_code_rec_id as zipcoderecid,
        valid_to as validto,
        validtotzid as validtotzid,
        valid_from as validfrom,
        validfromtzid as validfromtzid,
        null as districtname,
        street_id_ru as streetid_ru,
        house_id_ru as houseid_ru,
        flat_id_ru as flatid_ru,
        null as apartment_ru,
        null as building_ru,
        null as citykana_jp,
        is_private as isprivate,
        private_for_party as privateforparty,
        null as streetkana_jp,
        modifieddatetime as modifieddatetime,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as biseancodeid
    from d365_source

)

select * from renamed
