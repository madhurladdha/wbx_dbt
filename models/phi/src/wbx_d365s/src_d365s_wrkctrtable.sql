with d365_source as (
    select *
    from {{ source("D365S", "wrkctrtable") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (
    select
        'D365S' as source,
        wrkctrid as wrkctrid,
        name as name,
        wrkctrtype as wrkctrtype,
        effectivitypct as effectivitypct,
        operationschedpct as operationschedpct,
        capacity as capacity,
        capunit as capunit,
        vendid as vendid,
        cast(created as TIMESTAMP_NTZ) as created,
        queuetimebefore as queuetimebefore,
        setuptime as setuptime,
        processtime as processtime,
        processperqty as processperqty,
        transptime as transptime,
        queuetimeafter as queuetimeafter,
        transferbatch as transferbatch,
        tohours as tohours,
        errorpct as errorpct,
        setupcategoryid as setupcategoryid,
        processcategoryid as processcategoryid,
        bottleneckresource as bottleneckresource,
        caplimited as caplimited,
        capacitybatch as capacitybatch,
        propertylimited as propertylimited,
        exclusive as exclusive,
        qtycategoryid as qtycategoryid,
        routegroupid as routegroupid,
        wipissueledgerdimension as wipissueledgerdimension,
        wipvaluationledgerdimension as wipvaluationledgerdimension,
        resourceissueledgerdimension as resourceissueledgerdimension,
        resourceissueoffsetledgerdimension as resourceissueoffsetledgerdim,
        defaultdimension as defaultdimension,
        isindividualresource as isindividualresource,
        worker as worker,
        null as pmfsequencegroupid,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
