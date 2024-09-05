with
d365_source as (
    select *
    from {{ source("D365S", "ledger") }}
    where
        _fivetran_deleted = 'FALSE'
        and trim(upper(name)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (

    select
        'D365S' as source,
        chartofaccounts as chartofaccounts,
        upper(name) as name,
        description as description,
        primaryforlegalentity as primaryforlegalentity,
        fiscalcalendar as fiscalcalendar,
        defaultexchangeratetype as defaultexchangeratetype,
        budgetexchangeratetype as budgetexchangeratetype,
        accountingcurrency as accountingcurrency,
        reportingcurrency as reportingcurrency,
        isbudgetcontrolenabled as isbudgetcontrolenabled,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed

