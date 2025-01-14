

with source as (

    select * from {{ source('WEETABIX', 'projtable') }}

),

renamed as (

    select
        projgroupid,
        projid,
        name,
        projledgerposting,
        created,
        startdate,
        enddate,
        custaccount,
        dlvname,
        projinvoiceprojid,
        email,
        parentid,
        header,
        taxgroupid,
        format,
        checkbudget,
        sortingid,
        sortingid2_,
        sortingid3_,
        synclasttransid,
        status,
        wipproject,
        type,
        template,
        validateprojcategory,
        projlinepropertysearch,
        deliverylocation,
        defaultdimension,
        subject_sa,
        contractdate_sa,
        contractvalue_sa,
        contractamendment_sa,
        contractperiod_sa,
        requireactivityhourtrx,
        requireactivityexpensetrx,
        requireactivityitemtrx,
        requireactivityhourforecast,
        requireactivityexpenseforecast,
        requireactivityitemforecast,
        usebudgeting,
        usealternateproject,
        projpricegroup,
        projbudgetoverrunoption,
        syncentityid,
        syncversion,
        projbudgetarycontrolon,
        synccreatedbyexternal,
        assetid,
        projectedstartdate,
        projectedenddate,
        extensiondate,
        alternatebudgetproject,
        projbudgetinterval,
        timepostdetailsummary,
        mintimeincrement,
        projcarryforwardremainingbudget,
        projcarryforwardnegativebudget,
        workerresponsiblefinancial,
        workerresponsible,
        workerresponsiblesales,
        bankdocumenttype,
        jobid,
        jobpaytype,
        psaprojtask,
        psaunitid,
        psatimemeasure,
        psatrackcost,
        psaforecastmodelid,
        psainvoicemethod,
        psareadyforinvoicing,
        psadoinvoicecost,
        psanotes,
        psaprojstatus,
        psaforecastmodelidexternal,
        psaphone,
        psatelefax,
        psapreqitemvalidate,
        psapreqhourvalidate,
        psapreqcontrol,
        psaschedmilestone,
        psaschedstartdate,
        psaschedenddate,
        psaschedduration,
        psascheduseduration,
        psaschedcalendarid,
        psaschedignorecalendar,
        psaschedconstrainttype,
        psaschedconstraintdate,
        psaschedscheduled,
        psaretainscheduleid,
        psaretainpercent,
        psaretainincludelower,
        psaschedeffort,
        psaschedfromtime,
        psaschedtotime,
        psascheddefaultdate,
        psaresschedstatus,
        completescheduled,
        ocip,
        certifiedpayroll,
        ocipgl,
        workerpsaarchitect,
        projbudgetmanagement,
        synctocrm,
        modifieddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
