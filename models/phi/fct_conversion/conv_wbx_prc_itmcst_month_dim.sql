   
   {{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    )
}}

WITH old_fct AS 
(
    SELECT * FROM {{source('FACTS_FOR_COMPARE','prc_wtx_itmcst_month_dim')}} WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),
converted_fct as (
    select
        source_system  ,
        source_item_identifier  ,
        item_guid  ,
        source_business_unit_code  ,
        business_unit_address_guid  ,
        fiscal_period_number  ,
        input_uom  ,
        cost_method  ,
        phi_unit_cost  ,
        phi_currency_code  ,
        cost_rollfwd_flag  ,
        source_exchange_rate  ,
        base_unit_cost  ,
        base_currency_code  ,
        phi_conv_rt  
    from old_fct 

)

Select 
    cast(substring(source_system,1,255) as text(255) )            as source_system  ,
    cast(substring(source_item_identifier,1,60) as text(60) )     as source_item_identifier  ,
    cast({{ dbt_utils.surrogate_key(
    ["source_system","source_item_identifier"]) }} as text(255) ) as item_guid  ,
    cast(substring(source_business_unit_code,1,24) as text(24) )  as source_business_unit_code  ,
    cast({{ dbt_utils.surrogate_key(["source_system",
    "source_business_unit_code","'PLANT_DC'"]) }} as text(255) )  as business_unit_address_guid  ,
    cast(fiscal_period_number as number(38,0) )                   as fiscal_period_number  ,
    cast(substring(input_uom,1,6) as text(6) )                    as input_uom  ,
    cast(substring(cost_method,1,6) as text(6) )                  as cost_method  ,
    cast(phi_unit_cost as number(14,4) )                          as phi_unit_cost  ,
    cast(substring(phi_currency_code,1,6) as text(6) )            as phi_currency_code  ,
    cast(substring(cost_rollfwd_flag,1,1) as text(1) )            as cost_rollfwd_flag  ,
    cast(source_exchange_rate as number(14,7) )                   as source_exchange_rate  ,
    cast(base_unit_cost as number(14,4) )                         as base_unit_cost  ,
    cast(substring(base_currency_code,1,6) as text(6) )           as base_currency_code  ,
    cast(phi_conv_rt as number(14,7) )                            as phi_conv_rt,
    {{ dbt_utils.surrogate_key([
            "source_system",
            "source_item_identifier",
            "source_business_unit_code",
            "fiscal_period_number",
            "cost_method"
        ]) }}                                                     as unique_key
from converted_fct
