

with source as (

    select * from {{ source('WEETABIX', 'ledgerjournaltable') }}

),

renamed as (

    select
        journalnum,
        name,
        log,
        journaltype,
        workflowapprovalstatus,
        systemblocked,
        paymentsgenerated_it,
        userblockid,
        rejectedby,
        reportedasreadyby,
        journalname,
        posted,
        sessionlogindatetime,
        sessionlogindatetimetzid,
        offsetaccounttype,
        inuseby,
        groupblockid,
        originaljournalnum,
        currencycode,
        fixedexchrate,
        detailsummaryposting,
        documentnum,
        exchratesecondary,
        exchrate,
        eurotriangulation,
        fixedoffsetaccount,
        journaltotalcredit,
        journaltotaldebit,
        journaltotaloffsetbalance,
        removelineafterposting,
        currentoperationstax,
        ledgerjournalincltax,
        originalcompany,
        sessionid,
        bankremittancetype,
        bankaccountid,
        protestsettledbill,
        journalbalance,
        endbalance,
        custvendneginstprotestprocess,
        voucherallocatedatposting,
        numoflines,
        lineslimitbeforedistribution,
        posteddatetime,
        posteddatetimetzid,
        reverseentry,
        reversedate,
        defaultdimension,
        offsetledgerdimension,
        approver,
        numbersequencetable,
        assettransfertype_lt,
        retailstatementid,
        taxobligationcompany,
        modifiedby,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
