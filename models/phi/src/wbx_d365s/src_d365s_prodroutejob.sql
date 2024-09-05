/* 
    Necessary changes applied on 8/29/24 by Mike Traub once the D365S table was available.

    This D365S src model has been modified as the underlying source replication table does not yet exist.
    The source table doesn't build if there is no source system data.
    For now, this is still pulling from the D365 source but filtering with the condition of 0=1 so that no data is passed.
    This needs to have the following udpates once the D365S table itself exists:
        -Change the source to source('D365S', 'prodroutejob')
        -Remove the 0=1 filter
        -Update the field mapping according to the D365S field names.
*/

with d365_source as (
    select *
    from {{ source("D365S", "prodroutejob") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (

    select
            'D365' as source,
            prodid as prodid,
            oprnum as oprnum,
            numtype as numtype,
            jobtype as jobtype,
            link as link,
            linktype as linktype,
            oprpriority as oprpriority,
            schedcancelled as schedcancelled,
            jobcontrol as jobcontrol,
            wrkctrid as wrkctrid,
            fromdate as fromdate,
            fromtime as fromtime,
            todate as todate,
            totime as totime,
            jobstatus as jobstatus,
            null as propertyid,
            executedpct as executedpct,
            jobid as jobid,
            realizedstartdate as realizedstartdate,
            realizedstarttime as realizedstarttime,
            realizedenddate as realizedenddate,
            realizedendtime as realizedendtime,
            numsecondary as numsecondary,
            numprimary as numprimary,
            schedtimehours as schedtimehours,
            calctimehours as calctimehours,
            jobfinished as jobfinished,
            jobpaytype as jobpaytype,
            upper(dataareaid) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid

    from d365_source

)

select * from renamed
