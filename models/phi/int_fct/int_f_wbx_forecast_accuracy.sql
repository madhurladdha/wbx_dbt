{{ config(
    materialized=env_var("DBT_MAT_TABLE"),
    tags=["sales","performance","sales_performance"],
    ) }}

with 

cte_dim_date as (select * from {{ ref('src_dim_date') }}),

snapshot_matrix as (select * from {{ ref('src_forecast_accuracy_data_matrix_sheet') }}),

snapshot_list as (
    select distinct TO_DATE(month_1_) as snapshot
    from snapshot_matrix
    where current_date >= month_3
    union
    select distinct TO_DATE(month_2) as snapshot
    from snapshot_matrix
    where current_date >= month_3
    union
    select distinct TO_DATE(month_3) as snapshot
    from snapshot_matrix
     where current_date >= month_3
),

slsuber_actuals as (
    select * from {{ ref('fct_wbx_sls_uber') }}
    where source_content_filter in ('ACTUALS')
    and calendar_date >= (select fiscal_year_begin_dt from cte_dim_date where calendar_date = dateadd('year',-1,current_date))
),

forecast as 
(
     select * from {{ref('fct_wbx_sls_forecast')}} forecast
        join snapshot_list
        on forecast.snapshot_date = snapshot_list.snapshot
),

forecast_hist as (
    select * from {{ref('fct_wbx_sls_forecast_hist')}} forecast_hist
        join snapshot_list
        on forecast_hist.snapshot_date = snapshot_list.snapshot
),

forecast_ibe as (
    select * from {{ref('fct_wbx_sls_ibe_forecast')}} forecast_ibe
        join snapshot_list
        on forecast_ibe.snapshot_date = snapshot_list.snapshot
),

snapshot_list as (
    select distinct TO_DATE(month_1_) as snapshot
    from snapshot_matrix
    where current_date >= month_3
    union
    select distinct TO_DATE(month_2) as snapshot
    from snapshot_matrix
    where current_date >= month_3
    union
    select distinct TO_DATE(month_3) as snapshot
    from snapshot_matrix
     where current_date >= month_3
),


cy_forecast_combo as (
    select
        forecast.source_system,
        forecast.source_item_identifier,
        forecast.calendar_date,
        forecast.plan_source_customer_code,
        'WBX' as document_company, -- forecast cte is only WBX document company
        forecast.snapshot_date as snapshot_forecast_date,
        forecast.TOT_VOL_CA as fcf_tot_vol_ca,
        forecast.tot_vol_kg as fcf_tot_vol_kg,
        forecast.tot_vol_ul as fcf_tot_vol_ul,
        forecast.QTY_CA_EFFECTIVE_BASE_FC_SI as FCV_QTY_CA_EFFECTIVE_BASE_FC_SI,
        forecast.QTY_KG_EFFECTIVE_BASE_FC_SI as FCV_QTY_KG_EFFECTIVE_BASE_FC_SI,
        forecast.QTY_UL_EFFECTIVE_BASE_FC_SI as FCV_QTY_UL_EFFECTIVE_BASE_FC_SI,
        forecast.FCF_TOT_VOL_CA as FCF_TOT_ORIG_VOL_CA,
        forecast.FCF_TOT_VOL_KG as FCF_TOT_ORIG_VOL_KG,
        forecast.FCF_TOT_VOL_UL as FCF_TOT_ORIG_VOL_UL
    from forecast
    union
    select
        forecast_hist.source_system,
        forecast_hist.source_item_identifier,
        forecast_hist.calendar_date,
        forecast_hist.plan_source_customer_code,
        'WBX' as document_company, -- forecast cte is only WBX document company
        forecast_hist.snapshot_date as snapshot_forecast_date,
        forecast_hist.TOT_VOL_CA as fcf_tot_vol_ca,
        forecast_hist.tot_vol_kg as fcf_tot_vol_kg,
        forecast_hist.tot_vol_ul as fcf_tot_vol_ul,
        forecast_hist.QTY_CA_EFFECTIVE_BASE_FC_SI as FCV_QTY_CA_EFFECTIVE_BASE_FC_SI,
        forecast_hist.QTY_KG_EFFECTIVE_BASE_FC_SI as FCV_QTY_KG_EFFECTIVE_BASE_FC_SI,
        forecast_hist.QTY_UL_EFFECTIVE_BASE_FC_SI as FCV_QTY_UL_EFFECTIVE_BASE_FC_SI,
        forecast_hist.FCF_TOT_VOL_CA as FCF_TOT_ORIG_VOL_CA,
        forecast_hist.FCF_TOT_VOL_KG as FCF_TOT_ORIG_VOL_KG,
        forecast_hist.FCF_TOT_VOL_UL as FCF_TOT_ORIG_VOL_UL
    from forecast_hist
    union
    select
        forecast_ibe.source_system,
        forecast_ibe.source_item_identifier,
        forecast_ibe.calendar_date,
        forecast_ibe.plan_source_customer_code,
        'IBE' as document_company, -- forecast cte is only IBE document company
        forecast_ibe.snapshot_date as snapshot_forecast_date,
        forecast_ibe.TOT_VOL_CA as fcf_tot_vol_ca,
        forecast_ibe.tot_vol_kg as fcf_tot_vol_kg,
        forecast_ibe.tot_vol_ul as fcf_tot_vol_ul,
        forecast_ibe.QTY_CA_EFFECTIVE_BASE_FC_SI as FCV_QTY_CA_EFFECTIVE_BASE_FC_SI,
        forecast_ibe.QTY_KG_EFFECTIVE_BASE_FC_SI as FCV_QTY_KG_EFFECTIVE_BASE_FC_SI,
        forecast_ibe.QTY_UL_EFFECTIVE_BASE_FC_SI as FCV_QTY_UL_EFFECTIVE_BASE_FC_SI,
        forecast_ibe.FCF_TOT_VOL_CA as FCF_TOT_ORIG_VOL_CA,
        forecast_ibe.FCF_TOT_VOL_KG as FCF_TOT_ORIG_VOL_KG,
        forecast_ibe.FCF_TOT_VOL_UL as FCF_TOT_ORIG_VOL_UL
    from forecast_ibe
),

ly_forecast as (
    select
        'WEETABIX' as source_system,
        source_item_identifier,
        calendar_date,
        plan_source_customer_code,
        document_company,
        -- using actuals as LY_FCF fields
        sum(ly_shipped_ca_quantity) as ly_fcf_tot_vol_ca,
        sum(ly_shipped_kg_quantity) as ly_fcf_tot_vol_kg,
        sum(ly_shipped_ul_quantity) as ly_fcf_tot_vol_ul
    from slsuber_actuals
    group by source_system, source_item_identifier, calendar_date, plan_source_customer_code, document_company
)

select
    cy.source_system,
    cy.source_item_identifier,
    cy.calendar_date,
    cy.plan_source_customer_code,
    cy.document_company,
    cy.snapshot_forecast_date,
    cy.fcf_tot_vol_ca,
    cy.fcf_tot_vol_kg,
    cy.fcf_tot_vol_ul,
    cy.fcv_qty_ca_effective_base_fc_si,
    cy.fcv_qty_kg_effective_base_fc_si,
    cy.fcv_qty_ul_effective_base_fc_si,
    cy.FCF_TOT_ORIG_VOL_CA,
    cy.FCF_TOT_ORIG_VOL_KG,
    cy.FCF_TOT_ORIG_VOL_UL,
    ly.ly_fcf_tot_vol_ca,
    ly.ly_fcf_tot_vol_kg,
    ly.ly_fcf_tot_vol_ul
from cy_forecast_combo cy
left join ly_forecast ly
    on cy.source_system = ly.source_system
    and cy.source_item_identifier = ly.source_item_identifier
    and cy.calendar_date = ly.calendar_date
    and cy.plan_source_customer_code = ly.plan_source_customer_code
    and cy.document_company = ly.document_company