with source as (

    select * from {{ source('WEETABIX', 'vendtrans') }}

),

renamed as (

    select
        accountnum,
        transdate,
        voucher,
        invoice,
        txt,
        amountcur,
        settleamountcur,
        amountmst,
        settleamountmst,
        currencycode,
        duedate,
        lastsettlevoucher,
        lastsettledate,
        closed,
        transtype,
        approved,
        paymid,
        exchadjustment,
        documentnum,
        documentdate,
        arrival,
        lastexchadj,
        correct,
        lastexchadjvoucher,
        lastexchadjrate,
        postingprofile,
        settlement,
        cancel,
        postingprofileclose,
        postingprofileapprove,
        postingprofilecancel,
        postingprofilereopen,
        thirdpartybankaccountid,
        companybankaccountid,
        paymreference,
        paymmode,
        tax1099date,
        tax1099amount,
        tax1099num,
        offsetrecid,
        journalnum,
        eurotriangulation,
        cashdisccode,
        prepayment,
        paymspec,
        vendexchadjustmentrealized,
        vendexchadjustmentunrealized,
        approveddate,
        promissorynoteid,
        promissorynotestatus,
        promissorynoteseqnum,
        bankremittancefileid,
        fixedexchrate,
        bankcentralbankpurposetext,
        bankcentralbankpurposecode,
        tax1099state,
        tax1099stateamount,
        settletax1099amount,
        settletax1099stateamount,
        defaultdimension,
        exchratesecond,
        exchrate,
        lastsettleaccountnum,
        lastsettlecompany,
        invoiceproject,
        reasonrefrecid,
        releasedatecomment,
        invoicereleasedate,
        invoicereleasedatetzid,
        vendpaymentgroup,
        remittancelocation,
        remittanceaddress,
        tax1099fields,
        banklcimportline,
        accountingevent,
        approver,
        reportingcurrencyamount,
        reportingexchadjustmentrlzed,
        reportingexchadjustmentunrlzed,
        lastexchadjratereporting,
        reportingcurrencycrossrate,
        exchadjustmentreporting,
        settleamountreporting,
        taxinvoicepurchid,
        tax1099recid,
        consessionsettlementid,
        rbovendtrans,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        modifiedtransactionid,
        createddatetime,
        del_createdtime,
        createdby,
        createdtransactionid,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed