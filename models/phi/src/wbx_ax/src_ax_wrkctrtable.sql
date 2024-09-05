

with source as (

    select * from {{ source('WEETABIX', 'wrkctrtable') }}

),

renamed as (

    select
        wrkctrid,
        name,
        wrkctrtype,
        effectivitypct,
        operationschedpct,
        capacity,
        capunit,
        vendid,
        created,
        queuetimebefore,
        setuptime,
        processtime,
        processperqty,
        transptime,
        queuetimeafter,
        transferbatch,
        tohours,
        errorpct,
        setupcategoryid,
        processcategoryid,
        bottleneckresource,
        caplimited,
        capacitybatch,
        propertylimited,
        exclusive,
        qtycategoryid,
        routegroupid,
        wipissueledgerdimension,
        wipvaluationledgerdimension,
        resourceissueledgerdimension,
        resourceissueoffsetledgerdim,
        defaultdimension,
        isindividualresource,
        worker,
        pmfsequencegroupid,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
