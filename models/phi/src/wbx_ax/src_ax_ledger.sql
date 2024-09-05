

with source as (

    select * from {{ source('WEETABIX', 'ledger') }}

),

renamed as (

    select
        chartofaccounts,
        name,
        description,
        primaryforlegalentity,
        fiscalcalendar,
        defaultexchangeratetype,
        budgetexchangeratetype,
        accountingcurrency,
        reportingcurrency,
        isbudgetcontrolenabled,
        modifieddatetime,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
