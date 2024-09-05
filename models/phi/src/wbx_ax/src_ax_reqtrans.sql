

with source as (

    select * from {{ source('WEETABIX', 'reqtrans') }}

),

renamed as (

    select
        itemid,
        covinventdimid,
        reqdate,
        direction,
        reftype,
        openstatus,
        qty,
        covqty,
        refid,
        keep,
        reqdatedlvorig,
        futuresdays,
        futuresmarked,
        oprnum,
        actionqtyadd,
        actiondays,
        actionmarked,
        actiontype,
        planversion,
        originalquantity,
        isderiveddirectly,
        priority,
        actiondate,
        futuresdate,
        inventtransorigin,
        bomrefrecid,
        markingrefinventtransorigin,
        level_,
        bomtype,
        itemrouteid,
        itembomid,
        isforecastpurch,
        lastplanrecid,
        reqtime,
        futurestime,
        supplydemandsubclassification,
        reqprocessid,
        intercompanyplannedorder,
        pmfplangroupprimaryissue,
        custaccountid,
        custgroupid,
        isdelayed,
        mcrpricetimefence,
        pdsexpirydate,
        pdssellabledays,
        pmfactionqtyadd,
        pmfcobyrefrecid,
        pmfplangroupid,
        pmfplangrouppriority,
        pmfplanningitemid,
        pmfplanprioritycurrent,
        requisitionline,
        dataareaid,
        recversion,
        partition,
        recid,
        isforceditembomid,
        isforceditemrouteid,
        futurescalculated

    from source

)

select * from renamed
