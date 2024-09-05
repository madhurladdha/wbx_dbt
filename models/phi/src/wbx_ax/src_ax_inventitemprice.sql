

with source as (

    select * from {{ source('WEETABIX', 'inventitemprice') }}

),

renamed as (

    select
        itemid,
        versionid,
        pricetype,
        inventdimid,
        markup,
        priceunit,
        price,
        pricecalcid,
        unitid,
        priceallocatemarkup,
        priceqty,
        stdcosttransdate,
        stdcostvoucher,
        costingtype,
        activationdate,
        priceseccur_ru,
        markupseccur_ru,
        modifieddatetime,
        createddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
