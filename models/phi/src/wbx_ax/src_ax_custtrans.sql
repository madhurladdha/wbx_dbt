with source as (

    select * from {{ source('WEETABIX', 'custtrans') }}

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
        lastexchadjvoucher,
        closed,
        transtype,
        approved,
        exchadjustment,
        documentnum,
        documentdate,
        lastexchadjrate,
        fixedexchrate,
        lastexchadj,
        correct,
        bankcentralbankpurposecode,
        bankcentralbankpurposetext,
        settlement,
        interest,
        collectionletter,
        defaultdimension,
        postingprofileclose,
        exchratesecond,
        banklcexportline,
        accountingevent,
        exchrate,
        lastsettleaccountnum,
        companybankaccountid,
        thirdpartybankaccountid,
        paymmode,
        paymreference,
        paymmethod,
        cashpayment,
        controlnum,
        deliverymode,
        postingprofile,
        offsetrecid,
        eurotriangulation,
        orderaccount,
        cashdisccode,
        prepayment,
        paymspec,
        custexchadjustmentrealized,
        custexchadjustmentunrealized,
        paymmanlackdate,
        paymmanbatch,
        paymid,
        billofexchangeid,
        billofexchangestatus,
        billofexchangeseqnum,
        bankremittancefileid,
        collectionlettercode,
        invoiceproject,
        lastsettlecompany,
        cancelledpayment,
        reasonrefrecid,
        reportingcurrencyamount,
        reportingexchadjustmentreal,
        reportingexchadjustmentunreal,
        lastexchadjratereporting,
        reportingcurrencycrossrate,
        exchadjustmentreporting,
        settleamountreporting,
        approver,
        taxinvoicesalesid,
        concessioncontractid,
        concessionsettlementid,
        retailcusttrans,
        retailstoreid,
        retailterminalid,
        retailtransactionid,
        custbillingclassification,
        mcrpaymorderid,
        directdebitmandate,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        modifiedtransactionid,
        createddatetime,
        del_createdtime,
        createdby,
        createdtransactionid,
        upper(trim(dataareaid)) as dataareaid,
        recversion,
        partition,
        recid,
        paymschedid

    from source

)

select * from renamed
