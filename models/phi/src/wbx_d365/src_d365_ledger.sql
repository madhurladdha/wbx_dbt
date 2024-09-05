with
    d365_source as (
        select *
        from {{ source("D365", "ledger") }}
        where _FIVETRAN_DELETED='FALSE' and trim(upper(NAME)) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),
renamed as (

      select
        'D365' as source,
        chart_of_accounts as chartofaccounts,
        upper(name) as name,
        description as description,
        primary_for_legal_entity as primaryforlegalentity,
        fiscal_calendar as fiscalcalendar,
        default_exchange_rate_type as defaultexchangeratetype,
        budget_exchange_rate_type as budgetexchangeratetype,
        accounting_currency as accountingcurrency,
        reporting_currency as reportingcurrency,
        is_budget_control_enabled as isbudgetcontrolenabled,
        modifieddatetime as modifieddatetime,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
      
)

select * from renamed

