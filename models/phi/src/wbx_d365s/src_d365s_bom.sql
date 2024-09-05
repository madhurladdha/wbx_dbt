with
d365_source as (
    select * from {{ source("D365S", "bom") }} where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365S' as source,
        cast(linenum as DECIMAL(32, 16)) as linenum,
        bomtype as bomtype,
        bomconsump as bomconsump,
        itemid as itemid,
        bomqty as bomqty,
        calculation as calculation,
        height as height,
        width as width,
        depth as depth,
        density as density,
        constant as constant,
        roundup as roundup,
        roundupqty as roundupqty,
        null as position,
        oprnum as oprnum,
        cast(fromdate as TIMESTAMP_NTZ) as fromdate,
        cast(todate as TIMESTAMP_NTZ) as todate,
        null as vendid,
        unitid as unitid,
        bomid as bomid,
        null as configgroupid,
        formula as formula,
        bomqtyserie as bomqtyserie,
        null as itembomid,
        null as itemrouteid,
        inventdimid as inventdimid,
        cast(scrapvar as NUMBER(32, 16)) as scrapvar,
        scrapconst as scrapconst,
        prodflushingprincip as prodflushingprincip,
        endschedconsump as endschedconsump,
        projsetsubprodtoconsumed as projsetsubprodtoconsumed,
        wrkctrconsumption as wrkctrconsumption,
        null as itempbaid,
        null as pdsbasevalue,
        pdscwqty as pdscwqty,
        pdsingredienttype as pdsingredienttype,
        pdsinheritcoproductbatchattrib as pdsinheritcoproductbatchattrib,
        pdsinheritcoproductshelflife as pdsinheritcoproductshelflife,
        pdsinheritenditembatchattrib as pdsinheritenditembatchattrib,
        pdsinheritenditemshelflife as pdsinheritenditemshelflife,
        pmfformulapct as pmfformulapct,
        pmfpctenable as pmfpctenable,
        null as pmfplangroupid,
        pmfplangrouppriority as pmfplangrouppriority,
        pmfscalable as pmfscalable,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
    where upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

)

select * from renamed
