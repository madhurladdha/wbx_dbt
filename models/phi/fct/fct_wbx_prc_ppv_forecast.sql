{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    tags = ["ppv","procurement","forecast","ppv_forecast"],
    on_schema_change='sync_all_columns',
    full_refresh=false,
    unique_key='version_dt', 
    incremental_strategy='delete+insert'
    )
}}

with old_table as
(
    select * from {{ref('conv_prc_ppv_wbx_forecast')}}  
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}

),
base_dim  as (
    select * from {{ ref ('stg_f_wbx_prc_ppv_forecast') }}
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}
),
old_model_conv as (
    select
        cast(substring(source_item_identifier,1,60) as text(60) ) as source_item_identifier  ,

        cast(substring(description,1,255) as text(255) ) as description  ,

        cast(substring(company_code,1,60) as text(60) ) as company_code  ,

        cast(version_dt as date) as version_dt  ,

        cast(substring(forecast_year,1,20) as text(20) ) as forecast_year  ,

        cast(substring(scenario,1,60) as text(60) ) as scenario  ,

        cast(calendar_date as date) as calendar_date  ,

        cast(item_guid as text(255) ) as item_guid  ,

        cast(substring(item_type,1,255) as text(255) ) as item_type  ,

        cast(substring(buyer_code,1,60) as text(60) ) as buyer_code  ,

        cast(substring(primary_uom,1,60) as text(60) ) as primary_uom  ,

        cast(quantity as number(38,10) ) as quantity  ,

        cast(price as number(38,10) ) as price  ,

        cast(substring(base_currency,1,10) as text(10) ) as base_currency  ,

        cast(load_date as timestamp_ntz(9) ) as load_date 
    from old_table
),
base_dim_conv as (
    Select
        cast(substring(source_item_identifier,1,60) as text(60) ) as source_item_identifier  ,

        cast(substring(description,1,255) as text(255) ) as description  ,

        cast(substring(company_code,1,60) as text(60) ) as company_code  ,

        cast(version_dt as date) as version_dt  ,

        cast(substring(forecast_year,1,20) as text(20) ) as forecast_year  ,

        cast(substring(scenario,1,60) as text(60) ) as scenario  ,

        cast(calendar_date as date) as calendar_date  ,

        cast(item_guid as text(255) ) as item_guid  ,

        cast(substring(item_type,1,255) as text(255) ) as item_type  ,

        cast(substring(buyer_code,1,60) as text(60) ) as buyer_code  ,

        cast(substring(primary_uom,1,60) as text(60) ) as primary_uom  ,

        cast(quantity as number(38,10) ) as quantity  ,

        cast(price as number(38,10) ) as price  ,

        cast(substring(base_currency,1,10) as text(10) ) as base_currency  ,

        cast(load_date as timestamp_ntz(9) ) as load_date
    from base_dim 
)
select * from old_model_conv
union
select * from base_dim_conv