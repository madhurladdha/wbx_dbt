

with source as (

    select * from {{ source('WEETABIX', 'paymterm') }}

),

renamed as (

    select
        paymtermid,
        paymmethod,
        numofdays,
        description,
        numofmonths,
        paymsched,
        cash,
        paymdayid,
        shipcarrierancillarycharge,
        postoffsettingar,
        shipcarriercertifiedcheck,
        creditcardpaymenttype,
        creditcardcreditcheck,
        cashledgerdimension,
        usedeliverydateforduedate_es,
        duedatelimitgroupid_es,
        useemplaccount_ru,
        additionalmonths,
        customerupdateduedate,
        cutoffday,
        defaultpaymterm_psn,
        vendorupdateduedate,
        dataareaid,
        recversion,
        partition,
        recid,
        cfmpaymentrequesttypepayment,
        cfmpaymentrequesttypeprepaym

    from source

)

select * from renamed
