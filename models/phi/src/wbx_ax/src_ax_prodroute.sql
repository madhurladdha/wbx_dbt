

with source as (

    select * from {{ source('WEETABIX', 'prodroute') }}

),

renamed as (

    select
        prodid,
        oprnum,
        level_,
        oprnumnext,
        oprid,
        queuetimebefore,
        setuptime,
        processtime,
        processperqty,
        transptime,
        queuetimeafter,
        overlapqty,
        errorpct,
        accerror,
        tohours,
        transferbatch,
        setupcategoryid,
        processcategoryid,
        oprfinished,
        formulafactor1,
        routetype,
        backorderstatus,
        propertyid,
        routegroupid,
        qtycategoryid,
        fromdate,
        fromtime,
        todate,
        totime,
        calcqty,
        calcsetup,
        calcproc,
        oprpriority,
        formula,
        routeoprrefrecid,
        defaultdimension,
        linktype,
        oprstartedup,
        executedprocess,
        executedsetup,
        constantreleased,
        phantombomfactor,
        wrkctridcost,
        jobidprocess,
        jobidsetup,
        jobpaytype,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
