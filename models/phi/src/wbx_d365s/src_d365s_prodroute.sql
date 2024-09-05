with d365_source as (
        select *
        from {{ source("D365S", "prodroute") }} where _FIVETRAN_DELETED='FALSE' AND upper(dataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),

    renamed as (

        select
            'D365S' as source,
            prodid as prodid,
            oprnum as oprnum,
            level as level_,
            oprnumnext as oprnumnext,
            oprid as oprid,
            queuetimebefore as queuetimebefore,
            setuptime as setuptime,
            processtime as processtime,
            processperqty as processperqty,
            transptime as transptime,
            queuetimeafter as queuetimeafter,
            overlapqty as overlapqty,
            errorpct as errorpct,
            accerror as accerror,
            tohours as tohours,
            transferbatch as transferbatch,
            setupcategoryid as setupcategoryid,
            processcategoryid as processcategoryid,
            oprfinished as oprfinished,
            formulafactor_1 as formulafactor1,
            routetype as routetype,
            backorderstatus as backorderstatus,
            null as propertyid,
            routegroupid as routegroupid,
            qtycategoryid as qtycategoryid,
            cast(fromdate as TIMESTAMP_NTZ) as fromdate,
            fromtime as fromtime,
            cast(todate as TIMESTAMP_NTZ) as todate,
            totime as totime,
            calcqty as calcqty,
            calcsetup as calcsetup,
            calcproc as calcproc,
            oprpriority as oprpriority,
            formula as formula,             
            routeoprrefrecid as routeoprrefrecid,             
            defaultdimension as defaultdimension,
            linktype as linktype,
            oprstartedup as oprstartedup,
            executedprocess as executedprocess,
            executedsetup as executedsetup,
            constantreleased as constantreleased,
            phantombomfactor as phantombomfactor,
            wrkctridcost as wrkctridcost,
            jobidprocess as jobidprocess,
            jobidsetup as jobidsetup,
            jobpaytype as jobpaytype,
            upper(dataareaid) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

        from d365_source

    )

select * from renamed
