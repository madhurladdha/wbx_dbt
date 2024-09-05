

with source as (

    select * from {{ source('WEETABIX', 'reqpo') }}

),

renamed as (

    select
        itemid,
        routejobsupdated,
        reqdate,
        qty,
        reqdateorder,
        vendid,
        itemgroupid,
        reqpostatus,
        purchunit,
        planversion,
        schedmethod,
        purchid,
        reqdatedlv,
        refid,
        reftype,
        itemrouteid,
        itembomid,
        itembuyergroupid,
        covinventdimid,
        reqtimeorder,
        vendgroupid,
        leadtime,
        calendardays,
        schedtodate,
        schedfromdate,
        purchqty,
        reqtime,
        bomroutecreated,
        isderiveddirectly,
        isforecastpurch,
        intvqr,
        intvmth,
        intvwk,
        costamount,
        transferid,
        product,
        pdscwreqqty,
        pmfbulkord,
        pmfplanningitemid,
        pmfsequenced,
        pmfyieldpct,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        dataareaid,
        recversion,
        partition,
        recid,
        wbxreqdlvtime,
        prodimported

    from source

)

select * from renamed
