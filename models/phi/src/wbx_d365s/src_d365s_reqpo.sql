with d365source as (
    select *
    from {{ source("D365S", "reqpo") }}
    where
        _fivetran_deleted = 'FALSE'
        and trim(upper(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        itemid as itemid,
        routejobsupdated as routejobsupdated,
        cast(reqdate as TIMESTAMP_NTZ) as reqdate,
        qty as qty,
        cast(reqdateorder as TIMESTAMP_NTZ) as reqdateorder,
        vendid as vendid,
        itemgroupid as itemgroupid,
        reqpostatus as reqpostatus,
        purchunit as purchunit,
        planversion as planversion,
        schedmethod as schedmethod,
        null as purchid,
        cast(reqdatedlv as TIMESTAMP_NTZ) as reqdatedlv,
        refid as refid,
        reftype as reftype,
        itemrouteid as itemrouteid,
        itembomid as itembomid,
        itembuyergroupid as itembuyergroupid,
        covinventdimid as covinventdimid,
        reqtimeorder as reqtimeorder,
        vendgroupid as vendgroupid,
        leadtime as leadtime,
        calendardays as calendardays,
        cast(schedtodate as TIMESTAMP_NTZ) as schedtodate,
        cast(schedfromdate as TIMESTAMP_NTZ) as schedfromdate,
        purchqty as purchqty,
        reqtime as reqtime,
        bomroutecreated as bomroutecreated,
        isderiveddirectly as isderiveddirectly,
        isforecastpurch as isforecastpurch,
        intvqr as intvqr,
        intvmth as intvmth,
        intvwk as intvwk,
        costamount as costamount,
        null as transferid,
        product as product,
        pdscwreqqty as pdscwreqqty,
        pmfbulkord as pmfbulkord,
        null as pmfplanningitemid,
        pmfsequenced as pmfsequenced,
        pmfyieldpct as pmfyieldpct,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        null as del_modifiedtime,
        modifiedby as modifiedby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxreqdlvtime,
        null as prodimported
    from d365source

)

select * from renamed