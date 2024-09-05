with d365_source as (
    select *
    from {{ source("D365S", "prodtable") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (
    select
        'D365S' as source,
        projlinkedtoorder as projlinkedtoorder,
        itemid as itemid,
        name as name,
        null as prodgroupid,
        prodstatus as prodstatus,
        prodprio as prodprio,
        prodlocked as prodlocked,
        prodtype as prodtype,
        schedstatus as schedstatus,
        cast(scheddate as TIMESTAMP_NTZ) as scheddate,
        qtysched as qtysched,
        qtystup as qtystup,
        cast(dlvdate as TIMESTAMP_NTZ) as dlvdate,
        cast(stupdate as TIMESTAMP_NTZ) as stupdate,
        cast(finisheddate as TIMESTAMP_NTZ) as finisheddate,
        cast(schedstart as TIMESTAMP_NTZ) as schedstart,
        cast(schedend as TIMESTAMP_NTZ) as schedend,
        defaultdimension as defaultdimension,
        height as height,
        width as width,
        depth as depth,
        density as density,
        qtycalc as qtycalc,
        cast(realdate as TIMESTAMP_NTZ) as realdate,
        reservation as reservation,
        prodpostingtype as prodpostingtype,
        inventtransid as inventtransid,
        inventreftype as inventreftype,
        null as inventrefid,
        null as inventreftransid,
        collectreflevel as collectreflevel,
        collectrefprodid as collectrefprodid,
        cast(bomdate as TIMESTAMP_NTZ) as bomdate,
        backorderstatus as backorderstatus,
        null as prodpoolid,
        profitset as profitset,
        cast(calcdate as TIMESTAMP_NTZ) as calcdate,
        routejobs as routejobs,
        checkroute as checkroute,
        null as propertyid,
        remaininventphysical as remaininventphysical,
        bomid as bomid,
        routeid as routeid,
        reqplanidsched as reqplanidsched,
        reqpoid as reqpoid,
        reflookup as reflookup,
        latestscheddirection as latestscheddirection,
        cast(latestscheddate as TIMESTAMP_NTZ) as latestscheddate,
        prodid as prodid,
        inventdimid as inventdimid,
        schedtotime as schedtotime,
        schedfromtime as schedfromtime,
        latestschedtime as latestschedtime,
        dlvtime as dlvtime,
        null as prodorigid,
        ganttcolorid as ganttcolorid,
        null as projid,
        projpostingtype as projpostingtype,
        null as projcategoryid,
        null as projlinepropertyid,
        null as projtransid,
        projcostprice as projcostprice,
        projcostamount as projcostamount,
        null as projsalescurrencyid,
        null as projsalesunitid,
        projsalesprice as projsalesprice,
        null as projtaxgroupid,
        null as projtaxitemgroupid,
        null as activitynumber,
        null as pricegroup_ru,
        null as currencycode_ru,
        null as pbaid,
        pmftotalcostallocation as pmftotalcostallocation,
        pdscwbatchest as pdscwbatchest,
        pdscwbatchsched as pdscwbatchsched,
        pdscwbatchsize as pdscwbatchsize,
        pdscwbatchstup as pdscwbatchstup,
        pdscwremaininventphysical as pdscwremaininventphysical,
        pmfbulkord as pmfbulkord,
        pmfcobyvarallow as pmfcobyvarallow,
        pmfconsordid as pmfconsordid,
        pmfreworkbatch as pmfreworkbatch,
        pmfyieldpct as pmfyieldpct,
        prodwhsreleasepolicy as prodwhsreleasepolicy,
        cast(releaseddate as TIMESTAMP_NTZ) as releaseddate,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        null as del_createdtime,
        createdby as createdby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as prodimported,
        null as qtymade,
        null as statuschangedby

    from d365_source

)

select * from renamed