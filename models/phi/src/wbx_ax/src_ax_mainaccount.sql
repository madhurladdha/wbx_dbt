with source as (

    select * from {{ source('WEETABIX', 'mainaccount') }}

),

renamed as (

    select
        mainaccountid,
        name,
        currencycode,
        type,
        accountcategoryref,
        debitcreditproposal,
        debitcreditcheck,
        debitcreditbalancedemand,
        closing,
        exchangeadjusted,
        monetary,
        mandatorypaymentreference,
        validatecurrency,
        userinfoid,
        validateuser,
        postingtype,
        validateposting,
        srucode,
        consolidationmainaccount,
        offsetledgerdimension,
        mainaccounttemplate,
        ledgerchartofaccounts,
        inflationadjustment_mx,
        repomotype_mx,
        adjustmentmethod_mx,
        transferyearendaccount_es,
        openingaccount,
        reportingaccounttype,
        unitofmeasure,
        closetype,
        finreportingexchangeratetype,
        finreportingtranslationtype,
        parentmainaccount_br,
        recversion,
        partition,
        recid,
        standardmainaccount_w

    from source

)

select * from renamed
