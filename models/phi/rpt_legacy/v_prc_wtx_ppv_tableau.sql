{{ config(tags=["procurement", "ppv"]) }}

with prc_wtx_ppv_fact as (select * from {{ ref('fct_wbx_prc_ppv') }}),
prc_wtx_item_categorization as (select * from {{ ref('dim_wbx_categorization') }}),
dim_date as (select * from {{ ref("src_dim_date") }}),
dim_date_oc as (select * from {{ ref('dim_wbx_date_oc') }}),
itm_item_master_dim as (select * from {{ ref('dim_wbx_item') }}),
adr_plant_dc_master_dim as (select * from {{ ref('dim_wbx_plant_dc') }})

select
    fact.source_content_filter,
    fact.po_order_number,
    fact.voucher,
    version_dt,
    fact.source_item_identifier as item_code,
    fact.price_unit as standard_price_unit,
    fact.company_code,
    nvl(cat.description, item.description) as item_description,
    cat.new_item_buyer_group as item_buyer_group,
    cat.new_buyer_name as buyer_name,
    cat.eur_flag,
    cat.approx_month_covered,
    item_type,
    primary_uom,
    buyer_code,
    fact.calendar_date,
    base_currency,
    txn_currency,
    cy_standard_cost,
    cy_base_receipt_cost,
    cy_receipt_received_quantity,
    cy_gl_adjustment_receipt as cy_ppv_from_receipt,
    cy_eur_receipt_cost,
    cy_eur_receipt_received_quantity,
    cy_curr_conv_rt,
    ly_standard_cost,
    ly_base_receipt_cost,
    ly_receipt_received_quantity,
    ly_gl_adjustment_receipt as ly_ppv_from_receipt,
    ly_eur_receipt_cost,
    ly_eur_receipt_received_quantity,
    ly_curr_conv_rt,
    invoiced_qty,
    invoiced_amount,
    cy_gl_adjustment_invoice as cy_ppv_from_invoice,
    ly_gl_adjustment_invoice as ly_ppv_from_invoice,
    budget_version_date,
    budget_forcast_year,
    budget_quantity,
    budget_price,
    budget_exchange_rate,
    forecast_version_date,
    forecast_year,
    forecast_quantity,
    forecast_price,
    forecast_exchange_rate,
    budget_inflation,
    forecast_inflation,
    dd_oc.uk_holiday_flag,
    dd_oc.day_of_month_business,
    dd_oc.day_of_month_actual,
    dt.report_fiscal_year,
    dt.report_fiscal_year_period_no,
    dt.fiscal_year_begin_dt,
    dt.fiscal_year_end_dt
from prc_wtx_ppv_fact fact
left join
    prc_wtx_item_categorization cat
    on fact.source_item_identifier = cat.source_item_identifier
    and fact.company_code = cat.company_code
left join dim_date dt on fact.calendar_date = dt.calendar_date
left join
    dim_date_oc dd_oc
    on fact.calendar_date = dd_oc.calendar_date
    and dd_oc.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
left join
    (
        select distinct itm.source_item_identifier, plant.company_code, itm.description
        from itm_item_master_dim itm
        inner join
            adr_plant_dc_master_dim plant
            on itm.source_business_unit_code = plant.source_business_unit_code
            and itm.source_system = plant.source_system
        where
            itm.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
            and itm.buyer_code is not null
            and itm.item_class
            in ('WHEAT', 'RAWMATS', 'PACKAGING', 'STRETCH', 'NONBOM', '3RDPARTY')
    ) item
    on fact.source_item_identifier = item.source_item_identifier
    and fact.company_code = item.company_code
