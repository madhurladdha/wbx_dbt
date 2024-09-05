with source as (

    select * from {{ source('WEETABIX', 'bom') }}

),

renamed as (

    select
        linenum,
        bomtype,
        bomconsump,
        itemid,
        bomqty,
        calculation,
        height,
        width,
        depth,
        density,
        constant,
        roundup,
        roundupqty,
        position,
        oprnum,
        fromdate,
        todate,
        vendid,
        unitid,
        bomid,
        configgroupid,
        formula,
        bomqtyserie,
        itembomid,
        itemrouteid,
        inventdimid,
        scrapvar,
        scrapconst,
        prodflushingprincip,
        endschedconsump,
        projsetsubprodtoconsumed,
        wrkctrconsumption,
        itempbaid,
        pdsbasevalue,
        pdscwqty,
        pdsingredienttype,
        pdsinheritcoproductbatchattrib,
        pdsinheritcoproductshelflife,
        pdsinheritenditembatchattrib,
        pdsinheritenditemshelflife,
        pmfformulapct,
        pmfpctenable,
        pmfplangroupid,
        pmfplangrouppriority,
        pmfscalable,
        modifieddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed