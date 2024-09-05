{{
    config(
        materialized='view'
    )
}}

with history as (

    select 
        a.*,
        '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system
    from {{ source('FACTS_FOR_COMPARE','prc_wtx_forecast_fact') }} a
    where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
)
select
    source_item_identifier  ,

    description  ,

    company_code  ,

    version_dt  ,

    forecast_year  ,

    scenario  ,

    calendar_date  ,

    {{ dbt_utils.surrogate_key(['source_system','source_item_identifier']) }} as item_guid  ,

    item_type  ,

    buyer_code  ,

    primary_uom  ,

    quantity  ,

    price  ,

    base_currency  ,

    load_date
from history