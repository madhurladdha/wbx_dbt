with d365_source as (
    select *
    from {{ source("D365S", "reqtrans") }}
    where
        _fivetran_deleted = 'FALSE'
        and trim(upper(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        itemid as itemid,
        covinventdimid as covinventdimid,
        cast(reqdate as TIMESTAMP_NTZ) as reqdate,
        direction as direction,
        reftype as reftype,
        openstatus as openstatus,
        qty as qty,
        covqty as covqty,
        refid as refid,
        keep as keep,
        cast(reqdatedlvorig as TIMESTAMP_NTZ) as reqdatedlvorig,
        futuresdays as futuresdays,
        futuresmarked as futuresmarked,
        oprnum as oprnum,
        actionqtyadd as actionqtyadd,
        actiondays as actiondays,
        actionmarked as actionmarked,
        actiontype as actiontype,
        planversion as planversion,
        originalquantity as originalquantity,
        isderiveddirectly as isderiveddirectly,
        priority as priority,
        cast(actiondate as TIMESTAMP_NTZ) as actiondate,
        cast(futuresdate as TIMESTAMP_NTZ) as futuresdate,
        inventtransorigin as inventtransorigin,
        bomrefrecid as bomrefrecid,
        markingrefinventtransorigin as markingrefinventtransorigin,
        level as level_,
        bomtype as bomtype,
        itemrouteid as itemrouteid,
        itembomid as itembomid,
        isforecastpurch as isforecastpurch,
        lastplanrecid as lastplanrecid,
        reqtime as reqtime,
        futurestime as futurestime,
        supplydemandsubclassification as supplydemandsubclassification,
        reqprocessid as reqprocessid,
        intercompanyplannedorder as intercompanyplannedorder,
        pmfplangroupprimaryissue as pmfplangroupprimaryissue,
        custaccountid as custaccountid,
        custgroupid as custgroupid,
        isdelayed as isdelayed,
        mcrpricetimefence as mcrpricetimefence,
        cast(pdsexpirydate as TIMESTAMP_NTZ) as pdsexpirydate,
        pdssellabledays as pdssellabledays,
        pmfactionqtyadd as pmfactionqtyadd,
        pmfcobyrefrecid as pmfcobyrefrecid,
        null as pmfplangroupid,
        pmfplangrouppriority as pmfplangrouppriority,
        null as pmfplanningitemid,
        pmfplanprioritycurrent as pmfplanprioritycurrent,
        requisitionline as requisitionline,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        isforceditembomid as isforceditembomid,
        isforceditemrouteid as isforceditemrouteid,
        futurescalculated as futurescalculated
    from d365_source

)

select * from renamed
