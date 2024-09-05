

with source as (

    select * from {{ source('WEETABIX', 'inventtablemodule') }}

),

renamed as (

    select
        itemid,
        moduletype,
        unitid,
        price,
        priceunit,
        markup,
        linedisc,
        multilinedisc,
        enddisc,
        taxitemgroupid,
        markupgroupid,
        pricedate,
        priceqty,
        allocatemarkup,
        overdeliverypct,
        underdeliverypct,
        suppitemgroupid,
        intercompanyblocked,
        taxwithholditemgroupheading_th,
        taxwithholdcalculate_th,
        maximumretailprice_in,
        priceseccur_ru,
        markupseccur_ru,
        pdspricingprecision,
        taxgstreliefcategory_my,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        createddatetime,
        del_createdtime,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
