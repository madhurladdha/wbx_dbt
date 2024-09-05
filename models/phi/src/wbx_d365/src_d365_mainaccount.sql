with d365_source as (
    select *
    from {{ source("D365", "main_account") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        cast(main_account_id as varchar(20)) as mainaccountid,
        name as name,
        currency_code as currencycode,
        type as type,
        account_category_ref as accountcategoryref,
        debit_credit_proposal as debitcreditproposal,
        debit_credit_check as debitcreditcheck,
        debit_credit_balance_demand as debitcreditbalancedemand,
        closing as closing,
        exchange_adjusted as exchangeadjusted,
        monetary as monetary,
        mandatory_payment_reference as mandatorypaymentreference,
        validate_currency as validatecurrency,
        null as userinfoid,
        validate_user as validateuser,
        posting_type as postingtype,
        validate_posting as validateposting,
        null as srucode,
        null as consolidationmainaccount,
        offset_ledger_dimension as offsetledgerdimension,
        main_account_template as mainaccounttemplate,
        ledger_chart_of_accounts as ledgerchartofaccounts,
        inflation_adjustment_mx as inflationadjustment_mx,
        repomo_type_mx as repomotype_mx,
        adjustment_method_mx as adjustmentmethod_mx,
        transfer_year_end_account_es as transferyearendaccount_es,
        opening_account as openingaccount,
        reporting_account_type as reportingaccounttype,
        unit_of_measure as unitofmeasure,
        close_type as closetype,
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
