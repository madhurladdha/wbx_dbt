{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    tags = ["ppv","procurement"],
    on_schema_change='sync_all_columns',
    full_refresh=false,
    unique_key='version_dt', 
    incremental_strategy='delete+insert'
        )
}}

with old_table as
(
    select * from {{ref('conv_wbx_prc_ppv')}}  
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}

),
base_fct  as (
    select * from {{ ref ('int_f_wbx_prc_ppv') }}
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}
),
old_model_conv as (
    select
        cast(substring(source_content_filter,1,60) as text(60) ) as source_content_filter  ,

    cast(substring(po_order_number,1,255) as text(255) ) as po_order_number  ,

    cast(substring(voucher,1,255) as text(255) ) as voucher  ,

    cast(version_dt as date) as version_dt  ,

    cast(substring(source_item_identifier,1,60) as text(60) ) as source_item_identifier  ,

    cast(price_unit as number(33,4) ) as price_unit  ,

    cast(substring(company_code,1,255) as text(255) ) as company_code  ,

    cast(substring(item_type,1,255) as text(255) ) as item_type  ,

    cast(substring(primary_uom,1,255) as text(255) ) as primary_uom  ,

    cast(substring(buyer_code,1,60) as text(60) ) as buyer_code  ,

    cast(calendar_date as date) as calendar_date  ,

    cast(substring(base_currency,1,10) as text(10) ) as base_currency  ,

    cast(substring(txn_currency,1,10) as text(10) ) as txn_currency  ,

    cast(po_received_date as date) as po_received_date  ,

    cast(cy_standard_cost as number(33,4) ) as cy_standard_cost  ,

    cast(cy_base_receipt_cost as number(38,12) ) as cy_base_receipt_cost  ,

    cast(cy_receipt_received_quantity as number(38,10) ) as cy_receipt_received_quantity  ,

    cast(cy_gl_adjustment_receipt as number(34,7) ) as cy_gl_adjustment_receipt  ,

    cast(cy_eur_receipt_cost as number(38,12) ) as cy_eur_receipt_cost  ,

    cast(cy_eur_receipt_received_quantity as number(38,10) ) as cy_eur_receipt_received_quantity  ,

    cast(cy_curr_conv_rt as number(38,12) ) as cy_curr_conv_rt  ,

    cast(ly_standard_cost as number(33,4) ) as ly_standard_cost  ,

    cast(ly_base_receipt_cost as number(38,12) ) as ly_base_receipt_cost  ,

    cast(ly_receipt_received_quantity as number(38,10) ) as ly_receipt_received_quantity  ,

    cast(ly_gl_adjustment_receipt as number(34,7) ) as ly_gl_adjustment_receipt  ,

    cast(ly_eur_receipt_cost as number(38,12) ) as ly_eur_receipt_cost  ,

    cast(ly_eur_receipt_received_quantity as number(38,10) ) as ly_eur_receipt_received_quantity  ,

    cast(ly_curr_conv_rt as number(38,12) ) as ly_curr_conv_rt  ,

    cast(budget_version_date as date) as budget_version_date  ,

    cast(substring(budget_forcast_year,1,20) as text(20) ) as budget_forcast_year  ,

    cast(budget_quantity as number(38,10) ) as budget_quantity  ,

    cast(budget_price as number(38,10) ) as budget_price  ,

    cast(budget_exchange_rate as number(28,9) ) as budget_exchange_rate  ,

    cast(forecast_version_date as date) as forecast_version_date  ,

    cast(substring(forecast_year,1,20) as text(20) ) as forecast_year  ,

    cast(forecast_quantity as number(38,10) ) as forecast_quantity  ,

    cast(forecast_price as number(38,10) ) as forecast_price  ,

    cast(forecast_exchange_rate as number(28,9) ) as forecast_exchange_rate  ,

    cast(budget_inflation as number(28,9) ) as budget_inflation  ,

    cast(forecast_inflation as number(28,9) ) as forecast_inflation  ,

    cast(invoiced_qty as number(38,10) ) as invoiced_qty  ,

    cast(cy_gl_adjustment_invoice as number(34,7) ) as cy_gl_adjustment_invoice  ,

    cast(load_date as date) as load_date  ,

    cast(ly_gl_adjustment_invoice as number(34,7) ) as ly_gl_adjustment_invoice  ,

    cast(invoiced_amount as number(38,10) ) as invoiced_amount
          
    from old_table
),
base_fct_conv as (
    Select
        cast(substring(source_content_filter,1,60) as text(60) ) as source_content_filter  ,

    cast(substring(po_order_number,1,255) as text(255) ) as po_order_number  ,

    cast(substring(voucher,1,255) as text(255) ) as voucher  ,

    cast(version_dt as date) as version_dt  ,

    cast(substring(source_item_identifier,1,60) as text(60) ) as source_item_identifier  ,

    cast(price_unit as number(33,4) ) as price_unit  ,

    cast(substring(company_code,1,255) as text(255) ) as company_code  ,

    cast(substring(item_type,1,255) as text(255) ) as item_type  ,

    cast(substring(primary_uom,1,255) as text(255) ) as primary_uom  ,

    cast(substring(buyer_code,1,60) as text(60) ) as buyer_code  ,

    cast(calendar_date as date) as calendar_date  ,

    cast(substring(base_currency,1,10) as text(10) ) as base_currency  ,

    cast(substring(txn_currency,1,10) as text(10) ) as txn_currency  ,

    cast(po_received_date as date) as po_received_date  ,

    cast(cy_standard_cost as number(33,4) ) as cy_standard_cost  ,

    cast(cy_base_receipt_cost as number(38,12) ) as cy_base_receipt_cost  ,

    cast(cy_receipt_received_quantity as number(38,10) ) as cy_receipt_received_quantity  ,

    cast(cy_gl_adjustment_receipt as number(34,7) ) as cy_gl_adjustment_receipt  ,

    cast(cy_eur_receipt_cost as number(38,12) ) as cy_eur_receipt_cost  ,

    cast(cy_eur_receipt_received_quantity as number(38,10) ) as cy_eur_receipt_received_quantity  ,

    cast(cy_curr_conv_rt as number(38,12) ) as cy_curr_conv_rt  ,

    cast(ly_standard_cost as number(33,4) ) as ly_standard_cost  ,

    cast(ly_base_receipt_cost as number(38,12) ) as ly_base_receipt_cost  ,

    cast(ly_receipt_received_quantity as number(38,10) ) as ly_receipt_received_quantity  ,

    cast(ly_gl_adjustment_receipt as number(34,7) ) as ly_gl_adjustment_receipt  ,

    cast(ly_eur_receipt_cost as number(38,12) ) as ly_eur_receipt_cost  ,

    cast(ly_eur_receipt_received_quantity as number(38,10) ) as ly_eur_receipt_received_quantity  ,

    cast(ly_curr_conv_rt as number(38,12) ) as ly_curr_conv_rt  ,

    cast(budget_version_date as date) as budget_version_date  ,

    cast(substring(budget_forcast_year,1,20) as text(20) ) as budget_forcast_year  ,

    cast(budget_quantity as number(38,10) ) as budget_quantity  ,

    cast(budget_price as number(38,10) ) as budget_price  ,

    cast(nvl(budget_exchange_rate,1) as number(28,9) ) as budget_exchange_rate  ,

    cast(forecast_version_date as date) as forecast_version_date  ,

    cast(substring(forecast_year,1,20) as text(20) ) as forecast_year  ,

    cast(forecast_quantity as number(38,10) ) as forecast_quantity  ,

    cast(forecast_price as number(38,10) ) as forecast_price  ,

    cast(nvl(forecast_exchange_rate,1) as number(28,9) ) as forecast_exchange_rate  ,

    cast(nvl(budget_inflation,1) as number(28,9) ) as budget_inflation  ,

    cast(nvl(forecast_inflation,1) as number(28,9) ) as forecast_inflation  ,

    cast(invoiced_qty as number(38,10) ) as invoiced_qty  ,

    cast(cy_gl_adjustment_invoice as number(34,7) ) as cy_gl_adjustment_invoice  ,

    cast(load_date as date) as load_date  ,

    cast(ly_gl_adjustment_invoice as number(34,7) ) as ly_gl_adjustment_invoice  ,

    cast(invoiced_amount as number(38,10) ) as invoiced_amount
    from base_fct 
)
select * from old_model_conv
union
select * from base_fct_conv