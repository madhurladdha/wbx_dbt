

with source as (

    select * from {{ source('WEETABIX', 'inventitempricesim') }}

),

renamed as (

    select
        itemid,
        versionid,
        fromdate,
        pricetype,
        inventdimid,
        markup,
        priceunit,
        price,
        pricecalcid,
        unitid,
        priceallocatemarkup,
        priceqty,
        priceseccur_ru,
        markupseccur_ru,
        modifieddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed

