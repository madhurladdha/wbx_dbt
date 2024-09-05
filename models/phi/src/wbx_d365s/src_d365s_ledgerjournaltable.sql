with
d365_source as (
    select *
    from {{ source("D365S", "ledgerjournaltable") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        journalnum as journalnum,
        name as name,
        log as log,
        journaltype as journaltype,
        workflowapprovalstatus as workflowapprovalstatus,
        systemblocked as systemblocked,
        paymentsgenerated_it as paymentsgenerated_it,
        null as userblockid,
        rejectedby as rejectedby,
        reportedasreadyby as reportedasreadyby,
        journalname as journalname,
        posted as posted,
        --datetimes casted to Timestamp_ntz and tzid columns set to null
        cast(sessionlogindatetime as TIMESTAMP_NTZ) as sessionlogindatetime,
        null as sessionlogindatetimetzid,
        offsetaccounttype as offsetaccounttype,
        null as inuseby,
        null as groupblockid,
        originaljournalnum as originaljournalnum,
        currencycode as currencycode,
        fixedexchrate as fixedexchrate,
        detailsummaryposting as detailsummaryposting,
        null as documentnum,
        exchratesecondary as exchratesecondary,
        exchrate as exchrate,
        eurotriangulation as eurotriangulation,
        fixedoffsetaccount as fixedoffsetaccount,
        journaltotalcredit as journaltotalcredit,
        journaltotaldebit as journaltotaldebit,
        journaltotaloffsetbalance as journaltotaloffsetbalance,
        removelineafterposting as removelineafterposting,
        currentoperationstax as currentoperationstax,
        ledgerjournalincltax as ledgerjournalincltax,
        originalcompany as originalcompany,
        sessionid as sessionid,
        bankremittancetype as bankremittancetype,
        null as bankaccountid,
        protestsettledbill as protestsettledbill,
        journalbalance as journalbalance,
        endbalance as endbalance,
        custvendneginstprotestprocess as custvendneginstprotestprocess,
        voucherallocatedatposting as voucherallocatedatposting,
        numoflines as numoflines,
        lineslimitbeforedistribution as lineslimitbeforedistribution,
        cast(posteddatetime as TIMESTAMP_NTZ) as posteddatetime,
        null as posteddatetimetzid,
        reverseentry as reverseentry,
        cast(reversedate as TIMESTAMP_NTZ) as reversedate,
        defaultdimension as defaultdimension,
        offsetledgerdimension as offsetledgerdimension,
        approver as approver,
        numbersequencetable as numbersequencetable,
        assettransfertype_lt as assettransfertype_lt,
        null as retailstatementid,
        taxobligationcompany as taxobligationcompany,
        modifiedby as modifiedby,
        createdby as createdby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source


)

select * from renamed
