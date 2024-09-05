

with source as (

    select * from {{ source('WEETABIX', 'bomcalctrans') }}

),

renamed as (

    select
        costgroupid,
        level_,
        qty,
        costprice,
        costmarkup,
        salesprice,
        salesmarkup,
        transdate,
        linenum,
        resource_,
        unitid,
        oprid,
        inventdimstr,
        consumptionvariable,
        consumptionconstant,
        bom,
        oprnum,
        calctype,
        costpriceunit,
        costpriceqty,
        salespriceqty,
        costmarkupqty,
        salesmarkupqty,
        pricecalcid,
        numofseries,
        oprnumnext,
        oprpriority,
        consumptioninvent,
        inventdimid,
        vendid,
        consumptype,
        salespriceunit,
        netweightqty,
        infolog,
        salespricemodelused,
        pricediscqty,
        costpricemodelused,
        calcgroupid,
        costpricefallbackversion,
        salespricefallbackversion,
        routelevel,
        costpriceqtyseccur_ru,
        costmarkupqtyseccur_ru,
        costpriceseccur_ru,
        costmarkupseccur_ru,
        consistofprice,
        parentbomcalctrans,
        costcalculationmethod,
        createddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
