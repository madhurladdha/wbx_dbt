with d365_source as (
    select *
    from {{ source("D365S", "mainaccount") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        cast(mainaccountid as varchar(20)) as mainaccountid,
        name as name,
        currencycode as currencycode,
        type as type,
        accountcategoryref as accountcategoryref,
        debitcreditproposal as debitcreditproposal,
        debitcreditcheck as debitcreditcheck,
        debitcreditbalancedemand as debitcreditbalancedemand,
        closing as closing,
        exchangeadjusted as exchangeadjusted,
        monetary as monetary,
        mandatorypaymentreference as mandatorypaymentreference,
        validatecurrency as validatecurrency,
        null as userinfoid,
        validateuser as validateuser,
        postingtype as postingtype,
        validateposting as validateposting,
        null as srucode,
        null as consolidationmainaccount,
        offsetledgerdimension as offsetledgerdimension,
        mainaccounttemplate as mainaccounttemplate,
        ledgerchartofaccounts as ledgerchartofaccounts,
        inflationadjustment_mx as inflationadjustment_mx,
        repomotype_mx as repomotype_mx,
        adjustmentmethod_mx as adjustmentmethod_mx,
        transferyearendaccount_es as transferyearendaccount_es,
        openingaccount as openingaccount,
        reportingaccounttype as reportingaccounttype,
        unitofmeasure as unitofmeasure,
        closetype as closetype,
        null as finreportingexchangeratetype,
        null as finreportingtranslationtype,
        null as parentmainaccount_br,
        recversion as recversion,
        partition as partition,
        recid as recid,
        standardmainaccount_w as standardmainaccount_w
    from d365_source

)

select * from renamed
