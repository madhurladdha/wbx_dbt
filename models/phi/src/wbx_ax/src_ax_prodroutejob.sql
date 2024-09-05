

with source as (

    select * from {{ source('WEETABIX', 'prodroutejob') }}

),

renamed as (

    select
        prodid,
        oprnum,
        numtype,
        jobtype,
        link,
        linktype,
        oprpriority,
        schedcancelled,
        jobcontrol,
        wrkctrid,
        fromdate,
        fromtime,
        todate,
        totime,
        jobstatus,
        propertyid,
        executedpct,
        jobid,
        realizedstartdate,
        realizedstarttime,
        realizedenddate,
        realizedendtime,
        numsecondary,
        numprimary,
        schedtimehours,
        calctimehours,
        jobfinished,
        jobpaytype,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
