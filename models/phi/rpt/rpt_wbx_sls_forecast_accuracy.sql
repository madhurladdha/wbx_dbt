{{ config(
    materialized=env_var("DBT_MAT_VIEW"),
    tags=["sales","performance","sales_performance"],
    ) }}

/*  Forecast Accuracy Satellite Data Source View
This view is meant to be a trimmed down version from the main Performance view that includes the following to limit size and improve performance.
-The primary "fact" source is the Sales Uber table which has already been loaded up with a superset of relevant data for.
-Focuses on Actuals, Budgets, and Forecasts for all 3 Weetabix companies: WBX, IBE, and RFL
-Exluding All amount or price related fields and only including the Volume/Qty fields necessary.
-Denormalizes the measures as columns instead of rows under different source_content_filters.
-Aggregating up on the time dimension from the day level to 2 different grains: Planning Week and Calendar Month.
-All Customer data is at the Planning Customer (Trade Type) level.  So actuals are aggregated up from Ship-To Customer Account.
-Terms or Promotions, if in the Uber data, are not required here.
-GL Account grain is not required
*/

with 
cte_dim_date as (select * from {{ ref('src_dim_date') }}),
/* Uber as the primary source.  Filter down to just the required source_content_filters and for the current and previous Fiscal Year's data.
*/
cte_slsuber as (
    select * from {{ ref('fct_wbx_sls_uber') }}
    where source_content_filter in ('ACTUALS','BUDGET')
    and calendar_date >= (select fiscal_year_begin_dt from cte_dim_date where calendar_date = dateadd('year',-1,current_date))
),
snapshot_matrix as (select * from {{ ref('src_forecast_accuracy_data_matrix_sheet') }}),
cte_planning_date_oc as (select * from {{ ref('dim_wbx_planning_date_oc') }}),
cte_item_ext as (select * from {{ ref('dim_wbx_item_ext') }}),
cte_budget_reference as ( select * from {{ ref('src_sls_wtx_budget_scen_xref') }} ),
cte_v_wtx_cust_planning as ( SELECT * from {{ref('dim_wbx_cust_planning')}} ),
cte_sls_wtx_mape_targets as (select * from {{ ref('src_sls_wtx_mape_targets') }}),

/* For Actuals we only need the Sales Order data for Volumes.  Do NOT include actuals from the GL Allocations as those do not impact volume.
*/
slsuber_actuals as (select * from cte_slsuber 
where source_content_filter = 'ACTUALS'
),

slsuber_budget as (select * from cte_slsuber 
where source_content_filter = 'BUDGET'
),

slsuber_forecast as (select * from {{ ref('int_f_wbx_forecast_accuracy') }}
),

/* This ensures we have item and item attributes at the singular item level and not at the Item-Plant level which can cause cross join issues.
*/
itm_ext as (select source_system, source_item_identifier, max(dummy_product_flag) as dummy_product_flag, max(item_type) as item_type, max(branding_desc) as branding_desc, max(product_class_desc) as product_class_desc, max(sub_product_desc) as sub_product_desc
                , max(strategic_desc) as strategic_desc, max(power_brand_desc) as power_brand_desc, max(manufacturing_group_desc) as manufacturing_group_desc
                , max(category_desc) as category_desc, max(pack_size_desc) as pack_size_desc, max(sub_category_desc) as sub_category_desc
                , max(consumer_units_in_trade_units) as consumer_units_in_trade_units, max(promo_type_desc) as promo_type_desc, max(consumer_units) as consumer_units
                , max(description) as description
                from cte_item_ext 
                group by source_system, source_item_identifier),

/* Gathers the list of Snapshot Dates for Forecasts that will be required, based on the flat file maintained for each month.
    The filters around current date are to suppress future dates or forecasts that are not applicable for the given date.
*/
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

/* Captures the Forecast data at the month grain. */
forecast_month as (
    select 
        'MONTH' as date_grain,
        fcst.source_item_identifier,
        TO_DATE(DATE_TRUNC('month', fcst.calendar_date)) as calendar_date,
        fcst.plan_source_customer_code,
        document_company as company,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.fcf_tot_vol_ca ELSE 0 END) as fcf_tot_vol_ca_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.fcf_tot_vol_ca ELSE 0 END) as fcf_tot_vol_ca_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.fcf_tot_vol_ca ELSE 0 END) as fcf_tot_vol_ca_m3,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_1_) then fcst.fcf_tot_vol_kg else 0 end) as fcf_tot_vol_kg_m1,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_2) then fcst.fcf_tot_vol_kg else 0 end) as fcf_tot_vol_kg_m2,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_3) then fcst.fcf_tot_vol_kg else 0 end) as fcf_tot_vol_kg_m3,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_1_) then fcst.fcf_tot_vol_ul else 0 end) as fcf_tot_vol_ul_m1,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_2) then fcst.fcf_tot_vol_ul else 0 end) as fcf_tot_vol_ul_m2,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_3) then fcst.fcf_tot_vol_ul else 0 end) as fcf_tot_vol_ul_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCV_QTY_CA_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_CA_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCV_QTY_CA_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_CA_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCV_QTY_CA_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_CA_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCV_QTY_KG_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_KG_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCV_QTY_KG_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_KG_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCV_QTY_KG_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_KG_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCV_QTY_UL_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_UL_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCV_QTY_UL_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_UL_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCV_QTY_UL_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_UL_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCF_TOT_ORIG_VOL_CA ELSE 0 END) as FCF_TOT_ORIG_VOL_CA_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCF_TOT_ORIG_VOL_CA ELSE 0 END) as FCF_TOT_ORIG_VOL_CA_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCF_TOT_ORIG_VOL_CA ELSE 0 END) as FCF_TOT_ORIG_VOL_CA_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCF_TOT_ORIG_VOL_KG ELSE 0 END) as FCF_TOT_ORIG_VOL_KG_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCF_TOT_ORIG_VOL_KG ELSE 0 END) as FCF_TOT_ORIG_VOL_KG_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCF_TOT_ORIG_VOL_KG ELSE 0 END) as FCF_TOT_ORIG_VOL_KG_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCF_TOT_ORIG_VOL_UL ELSE 0 END) as FCF_TOT_ORIG_VOL_UL_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCF_TOT_ORIG_VOL_UL ELSE 0 END) as FCF_TOT_ORIG_VOL_UL_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCF_TOT_ORIG_VOL_UL ELSE 0 END) as FCF_TOT_ORIG_VOL_UL_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.LY_FCF_TOT_VOL_CA ELSE 0 END) as LY_FCF_TOT_VOL_CA_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.LY_FCF_TOT_VOL_CA ELSE 0 END) as LY_FCF_TOT_VOL_CA_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.LY_FCF_TOT_VOL_CA ELSE 0 END) as LY_FCF_TOT_VOL_CA_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.LY_FCF_TOT_VOL_KG ELSE 0 END) as LY_FCF_TOT_VOL_KG_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.LY_FCF_TOT_VOL_KG ELSE 0 END) as LY_FCF_TOT_VOL_KG_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.LY_FCF_TOT_VOL_KG ELSE 0 END) as LY_FCF_TOT_VOL_KG_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.LY_FCF_TOT_VOL_UL ELSE 0 END) as LY_FCF_TOT_VOL_UL_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.LY_FCF_TOT_VOL_UL ELSE 0 END) as LY_FCF_TOT_VOL_UL_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.LY_FCF_TOT_VOL_UL ELSE 0 END) as LY_FCF_TOT_VOL_UL_m3

    from slsuber_forecast fcst
    /* This join filters down to only the applicable Snapshot Forecast Dates. */
    join snapshot_list
        on fcst.snapshot_forecast_date = snapshot_list.snapshot
    left join snapshot_matrix as ref
        on TO_DATE(ref.month) = TO_DATE(DATE_TRUNC('month', fcst.calendar_date))
    group by date_grain, fcst.source_item_identifier, TO_DATE(DATE_TRUNC('month', fcst.calendar_date)), fcst.plan_source_customer_code, fcst.document_company
),

/* Captures the Actuals data at the month grain. 
    Aggregates from Ship-To Customer Account up to the Planning Customer (Trade Type)
*/
actuals_month as (
    select
        'MONTH' as date_grain,
        source_item_identifier,
        to_date(date_trunc('month', calendar_date)) as calendar_date,
        plan_source_customer_code,
        document_company as company,
        sum(cy_shipped_ca_quantity)  as cy_shipped_ca_quantity,
        sum(cy_shipped_kg_quantity) as cy_shipped_kg_quantity,
        sum(cy_shipped_ul_quantity) as cy_shipped_ul_quantity,
        sum(cy_ordered_ca_quantity) as cy_ordered_ca_quantity,
        sum(cy_ordered_kg_quantity) as cy_ordered_kg_quantity,
        sum(cy_ordered_ul_quantity) as cy_ordered_ul_quantity,
        sum(ly_shipped_ca_quantity) as ly_shipped_ca_quantity,
        sum(ly_shipped_kg_quantity) as ly_shipped_kg_quantity,
        sum(ly_shipped_ul_quantity) as ly_shipped_ul_quantity,
        sum(ly_ordered_ca_quantity) as ly_ordered_ca_quantity,
        sum(ly_ordered_kg_quantity) as ly_ordered_kg_quantity,
        sum(ly_ordered_ul_quantity) as ly_ordered_ul_quantity
    from slsuber_actuals
    group by date_grain, source_item_identifier, to_date(date_trunc('month', calendar_date)), plan_source_customer_code, document_company
),

-- to get distinct fiscal years for each month
fiscal_year_month as (
    select distinct 
        TO_DATE(DATE_TRUNC('month', calendar_date)) as calendar_date,
        report_fiscal_year,
        report_fiscal_year_period_no
    from cte_dim_date
),

-- adding fiscal year to budget xref file
budget_reference as (
    select * from cte_budget_reference
    left join fiscal_year_month
        on fiscal_year_month.calendar_date = cte_budget_reference.delineation_date
),

/* Captures the Budget data at the month grain. */
budget_month as (
    select
        'MONTH' as date_grain,
        budget.source_item_identifier,
        TO_DATE(DATE_TRUNC('month', budget.calendar_date)) as calendar_date,
        budget.plan_source_customer_code,
        budget.document_company as company,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('BUDGET%') THEN budget.fcf_tot_vol_ca ELSE 0
        END) as BUD_TOT_VOL_CA_BUDGET,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F1%') THEN budget.fcf_tot_vol_ca ELSE NULL
        END) as BUD_TOT_VOL_CA_F1,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F2%') THEN budget.fcf_tot_vol_ca ELSE NULL
        END) as BUD_TOT_VOL_CA_F2,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F3%') THEN budget.fcf_tot_vol_ca ELSE NULL
        END) as BUD_TOT_VOL_CA_F3,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('BUDGET%') THEN budget.fcf_tot_vol_kg ELSE 0
        END) as BUD_TOT_VOL_KG_BUDGET,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F1%') THEN budget.fcf_tot_vol_kg ELSE NULL
        END) as BUD_TOT_VOL_KG_F1,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F2%') THEN budget.fcf_tot_vol_kg ELSE NULL
        END) as BUD_TOT_VOL_KG_F2,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F3%') THEN budget.fcf_tot_vol_kg ELSE NULL
        END) as BUD_TOT_VOL_KG_F3,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('BUDGET%') THEN budget.fcf_tot_vol_ul ELSE 0
        END) as BUD_TOT_VOL_UL_BUDGET,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F1%') THEN budget.fcf_tot_vol_ul ELSE NULL
        END) as BUD_TOT_VOL_UL_F1,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F2%') THEN budget.fcf_tot_vol_ul ELSE NULL
        END) as BUD_TOT_VOL_UL_F2,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F3%') THEN budget.fcf_tot_vol_ul ELSE NULL
        END) as BUD_TOT_VOL_UL_F3
    from slsuber_budget budget
    left join budget_reference
        on budget.frozen_forecast = budget_reference.frozen_forecast
    left join fiscal_year_month
        ON TO_DATE(DATE_TRUNC('month', budget.calendar_date)) = TO_DATE(DATE_TRUNC('month', fiscal_year_month.calendar_date))
    group by date_grain, budget.source_item_identifier, TO_DATE(DATE_TRUNC('month', budget.calendar_date)), budget.plan_source_customer_code, budget.document_company
    ),

/*  This data set is meant to be the set of distinct key field combinations for the output at the MONTH level.
    This will be the anchor of the final join that brings all the data sets together: Actuals, Budgets, Forecasts.
*/

key_cte_month as (
select distinct 
        'WEETABIX' AS source_system,
        source_item_identifier,
        calendar_date,
        plan_source_customer_code,
        company as company
from forecast_month
union
select distinct 
        'WEETABIX' AS source_system,
        source_item_identifier,
        calendar_date,
        plan_source_customer_code,
        company as company
from actuals_month
union
select distinct 
        'WEETABIX' AS source_system,
        source_item_identifier,
        calendar_date,
        plan_source_customer_code,
        company as company
from budget_month
),

/* Captures the Forecast data at the Planning Week grain. */
forecast_week as (
    select 
        'WEEK' as date_grain,
        fcst.source_item_identifier,
        plan_cal.planning_week_start_dt as calendar_date,
        fcst.plan_source_customer_code,
        document_company as company,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.fcf_tot_vol_ca ELSE 0 END) as fcf_tot_vol_ca_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.fcf_tot_vol_ca ELSE 0 END) as fcf_tot_vol_ca_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.fcf_tot_vol_ca ELSE 0 END) as fcf_tot_vol_ca_m3,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_1_) then fcst.fcf_tot_vol_kg else 0 end) as fcf_tot_vol_kg_m1,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_2) then fcst.fcf_tot_vol_kg else 0 end) as fcf_tot_vol_kg_m2,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_3) then fcst.fcf_tot_vol_kg else 0 end) as fcf_tot_vol_kg_m3,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_1_) then fcst.fcf_tot_vol_ul else 0 end) as fcf_tot_vol_ul_m1,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_2) then fcst.fcf_tot_vol_ul else 0 end) as fcf_tot_vol_ul_m2,
        sum(case when to_date(fcst.snapshot_forecast_date) = to_date(ref.month_3) then fcst.fcf_tot_vol_ul else 0 end) as fcf_tot_vol_ul_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCV_QTY_CA_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_CA_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCV_QTY_CA_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_CA_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCV_QTY_CA_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_CA_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCV_QTY_KG_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_KG_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCV_QTY_KG_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_KG_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCV_QTY_KG_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_KG_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCV_QTY_UL_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_UL_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCV_QTY_UL_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_UL_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCV_QTY_UL_EFFECTIVE_BASE_FC_SI ELSE 0 END) as FCF_BASE_VOL_UL_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCF_TOT_ORIG_VOL_CA ELSE 0 END) as FCF_TOT_ORIG_VOL_CA_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCF_TOT_ORIG_VOL_CA ELSE 0 END) as FCF_TOT_ORIG_VOL_CA_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCF_TOT_ORIG_VOL_CA ELSE 0 END) as FCF_TOT_ORIG_VOL_CA_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCF_TOT_ORIG_VOL_KG ELSE 0 END) as FCF_TOT_ORIG_VOL_KG_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCF_TOT_ORIG_VOL_KG ELSE 0 END) as FCF_TOT_ORIG_VOL_KG_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCF_TOT_ORIG_VOL_KG ELSE 0 END) as FCF_TOT_ORIG_VOL_KG_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.FCF_TOT_ORIG_VOL_UL ELSE 0 END) as FCF_TOT_ORIG_VOL_UL_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.FCF_TOT_ORIG_VOL_UL ELSE 0 END) as FCF_TOT_ORIG_VOL_UL_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.FCF_TOT_ORIG_VOL_UL ELSE 0 END) as FCF_TOT_ORIG_VOL_UL_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.LY_FCF_TOT_VOL_CA ELSE 0 END) as LY_FCF_TOT_VOL_CA_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.LY_FCF_TOT_VOL_CA ELSE 0 END) as LY_FCF_TOT_VOL_CA_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.LY_FCF_TOT_VOL_CA ELSE 0 END) as LY_FCF_TOT_VOL_CA_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.LY_FCF_TOT_VOL_KG ELSE 0 END) as LY_FCF_TOT_VOL_KG_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.LY_FCF_TOT_VOL_KG ELSE 0 END) as LY_FCF_TOT_VOL_KG_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.LY_FCF_TOT_VOL_KG ELSE 0 END) as LY_FCF_TOT_VOL_KG_m3,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_1_) THEN fcst.LY_FCF_TOT_VOL_UL ELSE 0 END) as LY_FCF_TOT_VOL_UL_m1,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_2) THEN fcst.LY_FCF_TOT_VOL_UL ELSE 0 END) as LY_FCF_TOT_VOL_UL_m2,
        sum(case WHEN TO_DATE(fcst.snapshot_forecast_date) = TO_DATE(ref.month_3) THEN fcst.LY_FCF_TOT_VOL_UL ELSE 0 END) as LY_FCF_TOT_VOL_UL_m3

    from slsuber_forecast fcst
    /* This join filters down to only the applicable Snapshot Forecast Dates. */
    join snapshot_list
        on fcst.snapshot_forecast_date = snapshot_list.snapshot
    join cte_planning_date_oc as plan_cal
        on fcst.calendar_date = plan_cal.calendar_date
        and fcst.source_system = plan_cal.source_system
    left join snapshot_matrix as ref
        on TO_DATE(ref.month) = TO_DATE(DATE_TRUNC('month', fcst.calendar_date))
    -- where snapshot_forecast_date in () -- this will be list of distinct values in month-1,2,3
    group by date_grain, fcst.source_item_identifier, plan_cal.planning_week_start_dt, fcst.plan_source_customer_code, fcst.document_company
),

/* Captures the Actuals data at the WEEK grain. 
    Aggregates from Ship-To Customer Account up to the Planning Customer (Trade Type)
*/
actuals_week as (
    select
        'WEEK' as date_grain,
        source_item_identifier,
        plan_cal.planning_week_start_dt as calendar_date,
        plan_source_customer_code,
        document_company as company,
        sum(cy_shipped_ca_quantity)  as cy_shipped_ca_quantity,
        sum(cy_shipped_kg_quantity) as cy_shipped_kg_quantity,
        sum(cy_shipped_ul_quantity) as cy_shipped_ul_quantity,
        sum(cy_ordered_ca_quantity) as cy_ordered_ca_quantity,
        sum(cy_ordered_kg_quantity) as cy_ordered_kg_quantity,
        sum(cy_ordered_ul_quantity) as cy_ordered_ul_quantity,
        sum(ly_shipped_ca_quantity) as ly_shipped_ca_quantity,
        sum(ly_shipped_kg_quantity) as ly_shipped_kg_quantity,
        sum(ly_shipped_ul_quantity) as ly_shipped_ul_quantity,
        sum(ly_ordered_ca_quantity) as ly_ordered_ca_quantity,
        sum(ly_ordered_kg_quantity) as ly_ordered_kg_quantity,
        sum(ly_ordered_ul_quantity) as ly_ordered_ul_quantity
    from slsuber_actuals
    join cte_planning_date_oc as plan_cal
        on slsuber_actuals.calendar_date = plan_cal.calendar_date
        and slsuber_actuals.source_system = plan_cal.source_system
    group by date_grain, source_item_identifier, plan_cal.planning_week_start_dt, plan_source_customer_code, document_company
),

/* Captures the Budget data at the WEEK grain. */
budget_week as (
    select
        'WEEK' as date_grain,
        budget.source_item_identifier,
        plan_cal.planning_week_start_dt as calendar_date,
        budget.plan_source_customer_code,
        budget.document_company as company,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('BUDGET%') THEN budget.fcf_tot_vol_ca ELSE 0
        END) as BUD_TOT_VOL_CA_BUDGET,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F1%') THEN budget.fcf_tot_vol_ca ELSE NULL
        END) as BUD_TOT_VOL_CA_F1,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F2%') THEN budget.fcf_tot_vol_ca ELSE NULL
        END) as BUD_TOT_VOL_CA_F2,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F3%') THEN budget.fcf_tot_vol_ca ELSE NULL
        END) as BUD_TOT_VOL_CA_F3,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('BUDGET%') THEN budget.fcf_tot_vol_kg ELSE 0
        END) as BUD_TOT_VOL_KG_BUDGET,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F1%') THEN budget.fcf_tot_vol_kg ELSE NULL
        END) as BUD_TOT_VOL_KG_F1,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F2%') THEN budget.fcf_tot_vol_kg ELSE NULL
        END) as BUD_TOT_VOL_KG_F2,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F3%') THEN budget.fcf_tot_vol_kg ELSE NULL
        END) as BUD_TOT_VOL_KG_F3,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('BUDGET%') THEN budget.fcf_tot_vol_ul ELSE 0
        END) as BUD_TOT_VOL_UL_BUDGET,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F1%') THEN budget.fcf_tot_vol_ul ELSE NULL
        END) as BUD_TOT_VOL_UL_F1,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F2%') THEN budget.fcf_tot_vol_ul ELSE NULL
        END) as BUD_TOT_VOL_UL_F2,
        sum(case WHEN budget.calendar_date >= budget_reference.delineation_date and fiscal_year_month.report_fiscal_year = budget_reference.report_fiscal_year and budget.frozen_forecast like ('F3%') THEN budget.fcf_tot_vol_ul ELSE NULL
        END) as BUD_TOT_VOL_UL_F3
    from slsuber_budget budget
    join cte_planning_date_oc as plan_cal
        on budget.calendar_date = plan_cal.calendar_date
        and budget.source_system = plan_cal.source_system
    left join budget_reference
        on budget.frozen_forecast = budget_reference.frozen_forecast
    left join fiscal_year_month
        ON TO_DATE(DATE_TRUNC('month', budget.calendar_date)) = TO_DATE(DATE_TRUNC('month', fiscal_year_month.calendar_date))
    group by date_grain, budget.source_item_identifier, plan_cal.planning_week_start_dt, budget.plan_source_customer_code, budget.document_company
    ),


/*  This data set is meant to be the set of distinct key field combinations for the output at the MONTH level.
    This will be the anchor of the final join that brings all the data sets together later: Actuals, Budgets, Forecasts.
*/
-- key_cte_week as (
--     select distinct
--         cte_slsuber.source_system,
--         cte_slsuber.source_item_identifier,
--         plan_cal.planning_week_start_dt as calendar_date,
--         cte_slsuber.plan_source_customer_code,
--         cte_slsuber.document_company as company
--     from cte_slsuber
--     join cte_planning_date_oc as plan_cal
--         on cte_slsuber.calendar_date = plan_cal.calendar_date
--         and cte_slsuber.source_system = plan_cal.source_system
-- ),

key_cte_week as (
select distinct 
        'WEETABIX' AS source_system,
        source_item_identifier,
        calendar_date,
        plan_source_customer_code,
        company as company
from forecast_week
union
select distinct 
        'WEETABIX' AS source_system,
        source_item_identifier,
        calendar_date,
        plan_source_customer_code,
        company as company
from actuals_week
union
select distinct 
        'WEETABIX' AS source_system,
        source_item_identifier,
        calendar_date,
        plan_source_customer_code,
        company as company
from budget_week
),

/* Final full join and selection for MONTH grain.  This field list must match exactly with that of the WEEK grain data to follow.
*/
final_part1_month as (
select
    key_cte_month.source_system,
    key_cte_month.source_item_identifier,
    key_cte_month.calendar_date,
    key_cte_month.plan_source_customer_code,
    key_cte_month.company,
    'MONTH' as date_grain,
    forecast_month.fcf_tot_vol_ca_m1,
    forecast_month.fcf_tot_vol_ca_m2,
    forecast_month.fcf_tot_vol_ca_m3,
    forecast_month.fcf_tot_vol_ca_m1 * itm_ext.consumer_units as fcf_tot_vol_cu_m1,
    forecast_month.fcf_tot_vol_ca_m2 * itm_ext.consumer_units as fcf_tot_vol_cu_m2,
    forecast_month.fcf_tot_vol_ca_m3 * itm_ext.consumer_units as fcf_tot_vol_cu_m3,
    forecast_month.FCF_TOT_VOL_KG_m1,
    forecast_month.FCF_TOT_VOL_KG_m2,
    forecast_month.FCF_TOT_VOL_KG_m3,
    forecast_month.fcf_tot_vol_ca_m1 * itm_ext.consumer_units_in_trade_units as FCF_TOT_VOL_PK_m1,
    forecast_month.fcf_tot_vol_ca_m2 * itm_ext.consumer_units_in_trade_units as FCF_TOT_VOL_PK_m2,
    forecast_month.fcf_tot_vol_ca_m3 * itm_ext.consumer_units_in_trade_units as FCF_TOT_VOL_PK_m3,
    forecast_month.FCF_TOT_VOL_UL_m1,
    forecast_month.FCF_TOT_VOL_UL_m2,
    forecast_month.FCF_TOT_VOL_UL_m3,
    forecast_month.FCF_BASE_VOL_CA_m1,
    forecast_month.FCF_BASE_VOL_CA_m2,
    forecast_month.FCF_BASE_VOL_CA_m3,
    forecast_month.FCF_BASE_VOL_CA_m1 * itm_ext.consumer_units as FCF_BASE_VOL_CU_m1,
    forecast_month.FCF_BASE_VOL_CA_m2 * itm_ext.consumer_units as FCF_BASE_VOL_CU_m2,
    forecast_month.FCF_BASE_VOL_CA_m3 * itm_ext.consumer_units as FCF_BASE_VOL_CU_m3,
    forecast_month.FCF_BASE_VOL_KG_m1,
    forecast_month.FCF_BASE_VOL_KG_m2,
    forecast_month.FCF_BASE_VOL_KG_m3,
    forecast_month.FCF_BASE_VOL_CA_m1 * itm_ext.consumer_units_in_trade_units as FCF_BASE_VOL_PK_m1,
    forecast_month.FCF_BASE_VOL_CA_m2 * itm_ext.consumer_units_in_trade_units as FCF_BASE_VOL_PK_m2,
    forecast_month.FCF_BASE_VOL_CA_m3 * itm_ext.consumer_units_in_trade_units as FCF_BASE_VOL_PK_m3,
    forecast_month.FCF_BASE_VOL_UL_m1,
    forecast_month.FCF_BASE_VOL_UL_m2,
    forecast_month.FCF_BASE_VOL_UL_m3,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m1,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m2,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m3,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m1 * itm_ext.consumer_units as FCF_TOT_ORIG_VOL_CU_m1,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m2 * itm_ext.consumer_units as FCF_TOT_ORIG_VOL_CU_m2,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m3 * itm_ext.consumer_units as FCF_TOT_ORIG_VOL_CU_m3,
    forecast_month.FCF_TOT_ORIG_VOL_KG_m1,
    forecast_month.FCF_TOT_ORIG_VOL_KG_m2,
    forecast_month.FCF_TOT_ORIG_VOL_KG_m3,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m1 * itm_ext.consumer_units_in_trade_units as FCF_TOT_ORIG_VOL_PK_m1,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m2 * itm_ext.consumer_units_in_trade_units as FCF_TOT_ORIG_VOL_PK_m2,
    forecast_month.FCF_TOT_ORIG_VOL_CA_m3 * itm_ext.consumer_units_in_trade_units as FCF_TOT_ORIG_VOL_PK_m3,
    forecast_month.FCF_TOT_ORIG_VOL_UL_m1,
    forecast_month.FCF_TOT_ORIG_VOL_UL_m2,
    forecast_month.FCF_TOT_ORIG_VOL_UL_m3,
    forecast_month.LY_FCF_TOT_VOL_CA_m1,
    forecast_month.LY_FCF_TOT_VOL_CA_m2,
    forecast_month.LY_FCF_TOT_VOL_CA_m3,
    forecast_month.LY_FCF_TOT_VOL_CA_m1 * itm_ext.consumer_units as LY_FCF_TOT_VOL_CU_m1,
    forecast_month.LY_FCF_TOT_VOL_CA_m2 * itm_ext.consumer_units as LY_FCF_TOT_VOL_CU_m2,
    forecast_month.LY_FCF_TOT_VOL_CA_m3 * itm_ext.consumer_units as LY_FCF_TOT_VOL_CU_m3,
    forecast_month.LY_FCF_TOT_VOL_KG_m1,
    forecast_month.LY_FCF_TOT_VOL_KG_m2,
    forecast_month.LY_FCF_TOT_VOL_KG_m3,
    forecast_month.LY_FCF_TOT_VOL_CA_m1 * itm_ext.consumer_units_in_trade_units as LY_FCF_TOT_VOL_PK_m1,
    forecast_month.LY_FCF_TOT_VOL_CA_m2 * itm_ext.consumer_units_in_trade_units as LY_FCF_TOT_VOL_PK_m2,
    forecast_month.LY_FCF_TOT_VOL_CA_m3 * itm_ext.consumer_units_in_trade_units as LY_FCF_TOT_VOL_PK_m3,
    forecast_month.LY_FCF_TOT_VOL_UL_m1,
    forecast_month.LY_FCF_TOT_VOL_UL_m2,
    forecast_month.LY_FCF_TOT_VOL_UL_m3,
    actuals_month.cy_shipped_ca_quantity,
    actuals_month.cy_shipped_ca_quantity * itm_ext.consumer_units as cy_shipped_cu_quantity,
    actuals_month.cy_shipped_kg_quantity,
    actuals_month.cy_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units as cy_shipped_pk_quantity,
    actuals_month.cy_shipped_ul_quantity,
    actuals_month.cy_ordered_ca_quantity,
    actuals_month.cy_ordered_ca_quantity * itm_ext.consumer_units as cy_ordered_cu_quantity,
    actuals_month.cy_ordered_kg_quantity,
    actuals_month.cy_ordered_ca_quantity * itm_ext.consumer_units_in_trade_units as cy_ordered_pk_quantity,
    actuals_month.cy_ordered_ul_quantity,
    actuals_month.ly_shipped_ca_quantity,
    actuals_month.ly_shipped_ca_quantity * itm_ext.consumer_units as ly_shipped_cu_quantity,
    actuals_month.ly_shipped_kg_quantity,
    actuals_month.ly_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units as ly_shipped_pk_quantity,
    actuals_month.ly_shipped_ul_quantity,
    actuals_month.ly_ordered_ca_quantity,
    actuals_month.ly_ordered_ca_quantity * itm_ext.consumer_units as ly_ordered_cu_quantity,
    actuals_month.ly_ordered_kg_quantity,
    actuals_month.ly_ordered_ca_quantity * itm_ext.consumer_units_in_trade_units as ly_ordered_pk_quantity,
    actuals_month.ly_ordered_ul_quantity,
    budget_month.bud_tot_vol_ca_budget,
    coalesce(budget_month.bud_tot_vol_ca_f1, actuals_month.cy_shipped_ca_quantity) as bud_tot_vol_ca_f1,
    coalesce(budget_month.bud_tot_vol_ca_f2, actuals_month.cy_shipped_ca_quantity) as bud_tot_vol_ca_f2,
    coalesce(budget_month.bud_tot_vol_ca_f3, actuals_month.cy_shipped_ca_quantity) as bud_tot_vol_ca_f3,
    budget_month.bud_tot_vol_ca_budget * itm_ext.consumer_units as bud_tot_vol_cu_budget,
    coalesce(budget_month.bud_tot_vol_ca_f1 * itm_ext.consumer_units, actuals_month.cy_shipped_ca_quantity * itm_ext.consumer_units) as bud_tot_vol_cu_f1,
    coalesce(budget_month.bud_tot_vol_ca_f2 * itm_ext.consumer_units, actuals_month.cy_shipped_ca_quantity * itm_ext.consumer_units) as bud_tot_vol_cu_f2,
    coalesce(budget_month.bud_tot_vol_ca_f3 * itm_ext.consumer_units, actuals_month.cy_shipped_ca_quantity * itm_ext.consumer_units) as bud_tot_vol_cu_f3,
    budget_month.bud_tot_vol_kg_budget,
    coalesce(budget_month.bud_tot_vol_kg_f1, actuals_month.cy_shipped_kg_quantity) as bud_tot_vol_kg_f1,
    coalesce(budget_month.bud_tot_vol_kg_f2, actuals_month.cy_shipped_kg_quantity) as bud_tot_vol_kg_f2,
    coalesce(budget_month.bud_tot_vol_kg_f3, actuals_month.cy_shipped_kg_quantity) as bud_tot_vol_kg_f3,
    budget_month.bud_tot_vol_ca_budget * itm_ext.consumer_units_in_trade_units as bud_tot_vol_pk_budget,
    coalesce(budget_month.bud_tot_vol_ca_f1 * itm_ext.consumer_units_in_trade_units, actuals_month.cy_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units) as bud_tot_vol_pk_f1,
    coalesce(budget_month.bud_tot_vol_ca_f2 * itm_ext.consumer_units_in_trade_units, actuals_month.cy_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units) as bud_tot_vol_pk_f2,
    coalesce(budget_month.bud_tot_vol_ca_f3 * itm_ext.consumer_units_in_trade_units, actuals_month.cy_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units) as bud_tot_vol_pk_f3,
    budget_month.bud_tot_vol_ul_budget,
    coalesce(budget_month.bud_tot_vol_ul_f1, actuals_month.cy_shipped_ul_quantity) as bud_tot_vol_ul_f1,
    coalesce(budget_month.bud_tot_vol_ul_f2, actuals_month.cy_shipped_ul_quantity) as bud_tot_vol_ul_f2,
    coalesce(budget_month.bud_tot_vol_ul_f3, actuals_month.cy_shipped_ul_quantity) as bud_tot_vol_ul_f3,
    itm_ext.branding_desc,
    itm_ext.dummy_product_flag,
    itm_ext.sub_category_desc,
    itm_ext.item_type,
    itm_ext.manufacturing_group_desc,
    itm_ext.pack_size_desc,
    itm_ext.power_brand_desc,
    itm_ext.product_class_desc,
    itm_ext.strategic_desc,
    itm_ext.sub_product_desc,
    itm_ext.description,
    fiscal_year_month.report_fiscal_year,
    fiscal_year_month.report_fiscal_year_period_no,
    plan_cust.market as market,
    plan_cust.SUB_MARKET as submarket,
    plan_cust.TRADE_CLASS as trade_class,
    plan_cust.TRADE_GROUP as trade_group,
    plan_cust.TRADE_TYPE as trade_type,
    plan_cust.TRADE_SECTOR_DESC as trade_sector,
    mape_tgt.tgt_mape as tgt_mape,
    mape_tgt.tgt_bias as tgt_bias,
    /* Planning Calendar fields not applicable at MONTH grain.  But are for WEEK grain. */
    null as planning_week_code,
    null as planning_week_start_dt,
    null as planning_week_end_dt,
    null as planning_week_no,
    null as planning_month_code,
    null as planning_month_start_dt,
    null as planning_month_end_dt,
    null as planning_quarter_no,
    null as planning_quarter_start_dt,
    null as planning_quarter_end_dt,
    null as planning_year_no,
    null as planning_year_start_dt
    from key_cte_month
    left join forecast_month
        on forecast_month.source_item_identifier = key_cte_month.source_item_identifier
        and forecast_month.calendar_date = key_cte_month.calendar_date
        and forecast_month.plan_source_customer_code = key_cte_month.plan_source_customer_code
        and forecast_month.company = key_cte_month.company
    left join actuals_month
        on actuals_month.source_item_identifier = key_cte_month.source_item_identifier
        and actuals_month.calendar_date = key_cte_month.calendar_date
        and actuals_month.plan_source_customer_code = key_cte_month.plan_source_customer_code
        and actuals_month.company = key_cte_month.company
    left join budget_month
        on budget_month.source_item_identifier = key_cte_month.source_item_identifier
        and budget_month.calendar_date = key_cte_month.calendar_date
        and budget_month.plan_source_customer_code = key_cte_month.plan_source_customer_code
        and budget_month.company = key_cte_month.company
    left join itm_ext
        on itm_ext.source_item_identifier = key_cte_month.source_item_identifier
    left join fiscal_year_month
        on key_cte_month.calendar_date = fiscal_year_month.calendar_date
    left join cte_v_wtx_cust_planning plan_cust
        on trim(key_cte_month.plan_source_customer_code) = trim(plan_cust.trade_type_code)
        and key_cte_month.company= plan_cust.company_code
    ---Start MAPE Targets-----------------------------------------------------------------
    left join cte_sls_wtx_mape_targets mape_tgt
        on trim(key_cte_month.plan_source_customer_code) = trim(mape_tgt.trade_type_code)
        and key_cte_month.calendar_date between mape_tgt.eff_date and mape_tgt.expir_date
    ---End MAPE Targets-----------------------------------------------------------------
),

/* Final full join and selection for WEEK grain.  This field list must match exactly with that of the MONTH grain that proceeds.
*/
final_part2_week as 
(
    select
    key_cte_week.source_system,
    key_cte_week.source_item_identifier,
    key_cte_week.calendar_date,
    key_cte_week.plan_source_customer_code,
    key_cte_week.company,
    'WEEK' as date_grain,
    forecast_week.fcf_tot_vol_ca_m1,
    forecast_week.fcf_tot_vol_ca_m2,
    forecast_week.fcf_tot_vol_ca_m3,
    forecast_week.fcf_tot_vol_ca_m1 * itm_ext.consumer_units as fcf_tot_vol_cu_m1,
    forecast_week.fcf_tot_vol_ca_m2 * itm_ext.consumer_units as fcf_tot_vol_cu_m2,
    forecast_week.fcf_tot_vol_ca_m3 * itm_ext.consumer_units as fcf_tot_vol_cu_m3,
    forecast_week.FCF_TOT_VOL_KG_m1,
    forecast_week.FCF_TOT_VOL_KG_m2,
    forecast_week.FCF_TOT_VOL_KG_m3,
    forecast_week.fcf_tot_vol_ca_m1 * itm_ext.consumer_units_in_trade_units as FCF_TOT_VOL_PK_m1,
    forecast_week.fcf_tot_vol_ca_m2 * itm_ext.consumer_units_in_trade_units as FCF_TOT_VOL_PK_m2,
    forecast_week.fcf_tot_vol_ca_m3 * itm_ext.consumer_units_in_trade_units as FCF_TOT_VOL_PK_m3,
    forecast_week.FCF_TOT_VOL_UL_m1,
    forecast_week.FCF_TOT_VOL_UL_m2,
    forecast_week.FCF_TOT_VOL_UL_m3,
    forecast_week.FCF_BASE_VOL_CA_m1,
    forecast_week.FCF_BASE_VOL_CA_m2,
    forecast_week.FCF_BASE_VOL_CA_m3,
    forecast_week.FCF_BASE_VOL_CA_m1 * itm_ext.consumer_units as FCF_BASE_VOL_CU_m1,
    forecast_week.FCF_BASE_VOL_CA_m2 * itm_ext.consumer_units as FCF_BASE_VOL_CU_m2,
    forecast_week.FCF_BASE_VOL_CA_m3 * itm_ext.consumer_units as FCF_BASE_VOL_CU_m3,
    forecast_week.FCF_BASE_VOL_KG_m1,
    forecast_week.FCF_BASE_VOL_KG_m2,
    forecast_week.FCF_BASE_VOL_KG_m3,
    forecast_week.FCF_BASE_VOL_CA_m1 * itm_ext.consumer_units_in_trade_units as FCF_BASE_VOL_PK_m1,
    forecast_week.FCF_BASE_VOL_CA_m2 * itm_ext.consumer_units_in_trade_units as FCF_BASE_VOL_PK_m2,
    forecast_week.FCF_BASE_VOL_CA_m3 * itm_ext.consumer_units_in_trade_units as FCF_BASE_VOL_PK_m3,
    forecast_week.FCF_BASE_VOL_UL_m1,
    forecast_week.FCF_BASE_VOL_UL_m2,
    forecast_week.FCF_BASE_VOL_UL_m3,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m1,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m2,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m3,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m1 * itm_ext.consumer_units as FCF_TOT_ORIG_VOL_CU_m1,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m2 * itm_ext.consumer_units as FCF_TOT_ORIG_VOL_CU_m2,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m3 * itm_ext.consumer_units as FCF_TOT_ORIG_VOL_CU_m3,
    forecast_week.FCF_TOT_ORIG_VOL_KG_m1,
    forecast_week.FCF_TOT_ORIG_VOL_KG_m2,
    forecast_week.FCF_TOT_ORIG_VOL_KG_m3,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m1 * itm_ext.consumer_units_in_trade_units as FCF_TOT_ORIG_VOL_PK_m1,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m2 * itm_ext.consumer_units_in_trade_units as FCF_TOT_ORIG_VOL_PK_m2,
    forecast_week.FCF_TOT_ORIG_VOL_CA_m3 * itm_ext.consumer_units_in_trade_units as FCF_TOT_ORIG_VOL_PK_m3,
    forecast_week.FCF_TOT_ORIG_VOL_UL_m1,
    forecast_week.FCF_TOT_ORIG_VOL_UL_m2,
    forecast_week.FCF_TOT_ORIG_VOL_UL_m3,
    forecast_week.LY_FCF_TOT_VOL_CA_m1,
    forecast_week.LY_FCF_TOT_VOL_CA_m2,
    forecast_week.LY_FCF_TOT_VOL_CA_m3,
    forecast_week.LY_FCF_TOT_VOL_CA_m1 * itm_ext.consumer_units as LY_FCF_TOT_VOL_CU_m1,
    forecast_week.LY_FCF_TOT_VOL_CA_m2 * itm_ext.consumer_units as LY_FCF_TOT_VOL_CU_m2,
    forecast_week.LY_FCF_TOT_VOL_CA_m3 * itm_ext.consumer_units as LY_FCF_TOT_VOL_CU_m3,
    forecast_week.LY_FCF_TOT_VOL_KG_m1,
    forecast_week.LY_FCF_TOT_VOL_KG_m2,
    forecast_week.LY_FCF_TOT_VOL_KG_m3,
    forecast_week.LY_FCF_TOT_VOL_CA_m1 * itm_ext.consumer_units_in_trade_units as LY_FCF_TOT_VOL_PK_m1,
    forecast_week.LY_FCF_TOT_VOL_CA_m2 * itm_ext.consumer_units_in_trade_units as LY_FCF_TOT_VOL_PK_m2,
    forecast_week.LY_FCF_TOT_VOL_CA_m3 * itm_ext.consumer_units_in_trade_units as LY_FCF_TOT_VOL_PK_m3,
    forecast_week.LY_FCF_TOT_VOL_UL_m1,
    forecast_week.LY_FCF_TOT_VOL_UL_m2,
    forecast_week.LY_FCF_TOT_VOL_UL_m3,
    actuals_week.cy_shipped_ca_quantity,
    actuals_week.cy_shipped_ca_quantity * itm_ext.consumer_units as cy_shipped_cu_quantity,
    actuals_week.cy_shipped_kg_quantity,
    actuals_week.cy_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units as cy_shipped_pk_quantity,
    actuals_week.cy_shipped_ul_quantity,
    actuals_week.cy_ordered_ca_quantity,
    actuals_week.cy_ordered_ca_quantity * itm_ext.consumer_units as cy_ordered_cu_quantity,
    actuals_week.cy_ordered_kg_quantity,
    actuals_week.cy_ordered_ca_quantity * itm_ext.consumer_units_in_trade_units as cy_ordered_pk_quantity,
    actuals_week.cy_ordered_ul_quantity,
    actuals_week.ly_shipped_ca_quantity,
    actuals_week.ly_shipped_ca_quantity * itm_ext.consumer_units as ly_shipped_cu_quantity,
    actuals_week.ly_shipped_kg_quantity,
    actuals_week.ly_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units as ly_shipped_pk_quantity,
    actuals_week.ly_shipped_ul_quantity,
    actuals_week.ly_ordered_ca_quantity,
    actuals_week.ly_ordered_ca_quantity * itm_ext.consumer_units as ly_ordered_cu_quantity,
    actuals_week.ly_ordered_kg_quantity,
    actuals_week.ly_ordered_ca_quantity * itm_ext.consumer_units_in_trade_units as ly_ordered_pk_quantity,
    actuals_week.ly_ordered_ul_quantity,
    budget_week.bud_tot_vol_ca_budget,
    coalesce(budget_week.bud_tot_vol_ca_f1, actuals_week.cy_shipped_ca_quantity) as bud_tot_vol_ca_f1,
    coalesce(budget_week.bud_tot_vol_ca_f2, actuals_week.cy_shipped_ca_quantity) as bud_tot_vol_ca_f2,
    coalesce(budget_week.bud_tot_vol_ca_f3, actuals_week.cy_shipped_ca_quantity) as bud_tot_vol_ca_f3,
    budget_week.bud_tot_vol_ca_budget * itm_ext.consumer_units as bud_tot_vol_cu_budget,
    coalesce(budget_week.bud_tot_vol_ca_f1 * itm_ext.consumer_units, actuals_week.cy_shipped_ca_quantity * itm_ext.consumer_units) as bud_tot_vol_cu_f1,
    coalesce(budget_week.bud_tot_vol_ca_f2 * itm_ext.consumer_units, actuals_week.cy_shipped_ca_quantity * itm_ext.consumer_units) as bud_tot_vol_cu_f2,
    coalesce(budget_week.bud_tot_vol_ca_f3 * itm_ext.consumer_units, actuals_week.cy_shipped_ca_quantity * itm_ext.consumer_units) as bud_tot_vol_cu_f3,
    budget_week.bud_tot_vol_kg_budget,
    coalesce(budget_week.bud_tot_vol_kg_f1, actuals_week.cy_shipped_kg_quantity) as bud_tot_vol_kg_f1,
    coalesce(budget_week.bud_tot_vol_kg_f2, actuals_week.cy_shipped_kg_quantity) as bud_tot_vol_kg_f2,
    coalesce(budget_week.bud_tot_vol_kg_f3, actuals_week.cy_shipped_kg_quantity) as bud_tot_vol_kg_f3,
    budget_week.bud_tot_vol_ca_budget * itm_ext.consumer_units_in_trade_units as bud_tot_vol_pk_budget,
    coalesce(budget_week.bud_tot_vol_ca_f1 * itm_ext.consumer_units_in_trade_units, actuals_week.cy_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units) as bud_tot_vol_pk_f1,
    coalesce(budget_week.bud_tot_vol_ca_f2 * itm_ext.consumer_units_in_trade_units, actuals_week.cy_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units) as bud_tot_vol_pk_f2,
    coalesce(budget_week.bud_tot_vol_ca_f3 * itm_ext.consumer_units_in_trade_units, actuals_week.cy_shipped_ca_quantity * itm_ext.consumer_units_in_trade_units) as bud_tot_vol_pk_f3,
    budget_week.bud_tot_vol_ul_budget,
    coalesce(budget_week.bud_tot_vol_ul_f1, actuals_week.cy_shipped_ul_quantity) as bud_tot_vol_ul_f1,
    coalesce(budget_week.bud_tot_vol_ul_f2, actuals_week.cy_shipped_ul_quantity) as bud_tot_vol_ul_f2,
    coalesce(budget_week.bud_tot_vol_ul_f3, actuals_week.cy_shipped_ul_quantity) as bud_tot_vol_ul_f3,
    itm_ext.branding_desc,
    itm_ext.dummy_product_flag,
    itm_ext.sub_category_desc,
    itm_ext.item_type,
    itm_ext.manufacturing_group_desc,
    itm_ext.pack_size_desc,
    itm_ext.power_brand_desc,
    itm_ext.product_class_desc,
    itm_ext.strategic_desc,
    itm_ext.sub_product_desc,
    itm_ext.description,
    fiscal_year_month.report_fiscal_year,
    fiscal_year_month.report_fiscal_year_period_no,
    plan_cust.market as market,
    plan_cust.SUB_MARKET as submarket,
    plan_cust.TRADE_CLASS as trade_class,
    plan_cust.TRADE_GROUP as trade_group,
    plan_cust.TRADE_TYPE as trade_type,
    plan_cust.TRADE_SECTOR_DESC as trade_sector,
    mape_tgt.tgt_mape as tgt_mape,
    mape_tgt.tgt_bias as tgt_bias,
    /* Planning Calendar fields not applicable at MONTH grain.  But are for WEEK grain. */
    dtp.planning_week_code as planning_week_code,
    dtp.planning_week_start_dt as planning_week_start_dt,
    dtp.planning_week_end_dt as planning_week_end_dt,
    dtp.planning_week_no as planning_week_no,
    dtp.planning_month_code as planning_month_code,
    dtp.planning_month_start_dt as planning_month_start_dt,
    dtp.planning_month_end_dt as planning_month_end_dt,
    dtp.planning_quarter_no as planning_quarter_no,
    dtp.planning_quarter_start_dt as planning_quarter_start_dt,
    dtp.planning_quarter_end_dt as planning_quarter_end_dt,
    dtp.planning_year_no as planning_year_no,
    dtp.planning_year_start_dt as planning_year_start_dt
    from key_cte_week
    left join forecast_week
        on forecast_week.source_item_identifier = key_cte_week.source_item_identifier
        and forecast_week.calendar_date = key_cte_week.calendar_date
        and forecast_week.plan_source_customer_code = key_cte_week.plan_source_customer_code
        and forecast_week.company = key_cte_week.company
    left join actuals_week
        on actuals_week.source_item_identifier = key_cte_week.source_item_identifier
        and actuals_week.calendar_date = key_cte_week.calendar_date
        and actuals_week.plan_source_customer_code = key_cte_week.plan_source_customer_code
        and actuals_week.company = key_cte_week.company
    left join budget_week
        on budget_week.source_item_identifier = key_cte_week.source_item_identifier
        and budget_week.calendar_date = key_cte_week.calendar_date
        and budget_week.plan_source_customer_code = key_cte_week.plan_source_customer_code
        and budget_week.company = key_cte_week.company
    left join itm_ext
        on itm_ext.source_item_identifier = key_cte_week.source_item_identifier
    left join fiscal_year_month
        on key_cte_week.calendar_date = fiscal_year_month.calendar_date
    left join cte_v_wtx_cust_planning plan_cust
        on trim(key_cte_week.plan_source_customer_code) = trim(plan_cust.trade_type_code)
        and key_cte_week.company= plan_cust.company_code
    left join cte_planning_date_oc dtp
        on key_cte_week.source_system = dtp.source_system
        and key_cte_week.calendar_date = dtp.calendar_date
    ---Start MAPE Targets-----------------------------------------------------------------
    left join cte_sls_wtx_mape_targets mape_tgt
        on trim(key_cte_week.plan_source_customer_code) = trim(mape_tgt.trade_type_code)
        and key_cte_week.calendar_date between mape_tgt.eff_date and mape_tgt.expir_date
    ---End MAPE Targets-----------------------------------------------------------------
),

final as 
(
    select * from final_part1_month
    union 
    select * from final_part2_week
)

select * from final