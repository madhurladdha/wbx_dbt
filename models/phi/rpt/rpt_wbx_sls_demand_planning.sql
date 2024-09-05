{{ config(
    materialized=env_var("DBT_MAT_VIEW"),
    tags=["sales","performance","sales_performance"]
    ) }}

/*  Demand Planning Satellite Data Source View
This view is meant to be a trimmed down version from the main Performance view that includes the following to limit size and improve performance.
-The primary "fact" source is the Sales Uber table which has already been loaded up with a superset of relevant data for.
-Focuses on Actuals, Budgets, and Forecasts for all 3 Weetabix companies: WBX, IBE, and RFL
-Exluding All amount or price related fields and only including the Volume/Qty fields necessary.
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
    where
        source_content_filter in ('ACTUALS', 'FORECAST', 'BUDGET')
        and calendar_date
        >= (
            select fiscal_year_begin_dt
            from cte_dim_date
            where calendar_date = dateadd('year', -1, current_date)
        )
),

cte_planning_date_oc as (select * from {{ ref('dim_wbx_planning_date_oc') }}),

cte_item_ext as (select * from {{ ref('dim_wbx_item_ext') }}),

cte_v_wtx_cust_planning as (select * from {{ ref('dim_wbx_cust_planning') }}),

cte_exc_fact_account_plan_actual as (
    select * from {{ ref('src_exc_fact_account_plan_actual') }}
),

cte_exc_fact_account_plan as (
    select * from {{ ref('src_exc_fact_account_plan') }}
),

cte_exc_dim_scenario as (select * from {{ ref('src_exc_dim_scenario') }}),

cte_exc_dim_pc_customer as (select * from {{ ref('src_exc_dim_pc_customer') }}),

cte_exc_dim_pc_product as (select * from {{ ref('src_exc_dim_pc_product') }}),

cte_lkp_snapshot_date as (
    select * from {{ ref('stg_d_wtx_lkp_snapshot_date') }}
),

cte_sls_wtx_budget_scen_xref as (
    select *
    from {{ ref('src_sls_wtx_budget_scen_xref') }}
    where not contains(frozen_forecast, 'TEST')
),

cte_v_sls_wtx_onpromo as (
    select distinct
        trim(cust.code) as customer,
        trim(prod.code) as source_item_identifier,
        date_trunc('week', to_date(to_char(fact.day_idx), 'yyyymmdd') + 1)
        - 1 as date
    from
        (
            select * from cte_exc_fact_account_plan_actual where
                date_trunc('week', to_date(to_char(day_idx), 'yyyymmdd') + 1)
                - 1
                <= date_trunc('week', current_date) - 1
            union
            select * from cte_exc_fact_account_plan where
                date_trunc('week', to_date(to_char(day_idx), 'yyyymmdd') + 1)
                - 1
                <= date_trunc('week', current_date) - 1
        ) as fact
    left outer join cte_exc_dim_scenario as scen
        on fact.scen_idx = scen.scen_idx
    left outer join cte_exc_dim_pc_customer as cust
        on fact.cust_idx = cust.idx
    left outer join cte_exc_dim_pc_product as prod
        on fact.sku_idx = prod.idx
    where
        scen.scen_idx = 1
        and fact.isonpromo_si = true
),

/* For Actuals we only need the Sales Order data for Volumes.  Do NOT include actuals from the GL Allocations as those do not impact volume.
*/
slsuber_actuals as (
    select * from cte_slsuber
    where source_content_filter = 'ACTUALS'
),

slsuber_forecast as (
    select * from cte_slsuber
    where source_content_filter = 'FORECAST'
),

slsuber_budget as (
    select * from cte_slsuber
    where source_content_filter = 'BUDGET'
),

/* This ensures we have item and item attributes at the singular item level and not at the Item-Plant level which can cause cross join issues.
*/
itm_ext as (
    select
        source_system,
        source_item_identifier,
        max(dummy_product_flag) as dummy_product_flag,
        max(item_type) as item_type,
        max(branding_desc) as branding_desc,
        max(product_class_desc) as product_class_desc,
        max(sub_product_desc) as sub_product_desc,
        max(strategic_desc) as strategic_desc,
        max(power_brand_desc) as power_brand_desc,
        max(manufacturing_group_desc) as manufacturing_group_desc,
        max(category_desc) as category_desc,
        max(pack_size_desc) as pack_size_desc,
        max(sub_category_desc) as sub_category_desc,
        max(consumer_units_in_trade_units) as consumer_units_in_trade_units,
        max(promo_type_desc) as promo_type_desc,
        max(consumer_units) as consumer_units,
        max(description) as description
    from cte_item_ext
    group by source_system, source_item_identifier
),

saturday_week_list as (
    select
        calendar_date,
        calendar_day_of_week,
        snapshot_date,
        dense_rank()
            over (order by calendar_date desc)
            as week_rank
    from cte_planning_date_oc
    left join cte_lkp_snapshot_date
    where
        calendar_date <= snapshot_date
        and calendar_day_of_week = 'Saturday'
),

current_snapshot as (
    select
        calendar_date as current_snapshot,
        snapshot_date
    from saturday_week_list
    where week_rank = 1
),

-- selecting distinct list of months based on current_snapshot, ranking is used to select month_minus3
planning_month_list as (
    select distinct
        planning_cal.planning_month_code,
        planning_cal.planning_month_end_dt,
        snapshot.current_snapshot,
        dense_rank()
            over (order by planning_cal.planning_month_code desc)
            as month_rank
    from cte_planning_date_oc as planning_cal
    left join current_snapshot as snapshot
    where to_date(planning_cal.calendar_date) <= to_date(current_snapshot)
),

-- selecting just month_minus3
month_minus3 as (
    select
        planning_month_code,
        planning_month_end_dt,
        current_snapshot,
        month_rank
    from planning_month_list
    where month_rank = 4
),

-- building all snapshots we want to keep
cte_snapshot_list as (
    select
        snapshot.snapshot_date as live_snapshot,
        snapshot.current_snapshot,
        dateadd('week', -1, snapshot.current_snapshot) as "week_minus1",
        dateadd('week', -2, snapshot.current_snapshot) as "week_minus2",
        dateadd('week', -3, snapshot.current_snapshot) as "week_minus3",
        dateadd('week', -4, snapshot.current_snapshot) as "week_minus4",
        dateadd('week', -5, snapshot.current_snapshot) as "week_minus5",
        dateadd('week', -6, snapshot.current_snapshot) as "week_minus6",
        month_minus3.planning_month_end_dt as "month_minus3"
    from current_snapshot as snapshot
    left join month_minus3
        on month_minus3.current_snapshot = snapshot.current_snapshot
),

-- union snapshots into a list
snapshot_union as (
    select live_snapshot as snapshot
    from cte_snapshot_list
    union
    select current_snapshot as snapshot
    from cte_snapshot_list
    union
    select "week_minus1" as snapshot
    from cte_snapshot_list
    union
    select "week_minus2" as snapshot
    from cte_snapshot_list
    union
    select "week_minus3" as snapshot
    from cte_snapshot_list
    union
    select "week_minus4" as snapshot
    from cte_snapshot_list
    union
    select "week_minus5" as snapshot
    from cte_snapshot_list
    union
    select "week_minus6" as snapshot
    from cte_snapshot_list
    union
    select "month_minus3" as snapshot
    from cte_snapshot_list
),

-- aggregating to source_system, source_item_identifier, calendar_date, source_content_filter, plan_source_customer_code, company, and snapshot_forecast_date
cte_forecast as (
    select
        source_system,
        source_item_identifier,
        calendar_date,
        source_content_filter,
        plan_source_customer_code,
        document_company as company,
        snapshot_forecast_date,
        cy_line_actual_ship_date,
        cy_scheduled_ship_date,
        line_status_code,
        frozen_forecast,
        sum(fcf_tot_vol_ca) as fcf_tot_vol_ca,
        sum(fcf_tot_vol_kg) as fcf_tot_vol_kg,
        sum(fcf_tot_vol_ul) as fcf_tot_vol_ul,
        sum(fcv_qty_ca_effective_base_fc_si) as fcf_base_vol_ca,
        sum(fcv_qty_kg_effective_base_fc_si) as fcf_base_vol_kg,
        sum(fcv_qty_ul_effective_base_fc_si) as fcf_base_vol_ul,
        sum(fcf_tot_orig_vol_ca) as fcf_tot_orig_vol_ca,
        sum(fcf_tot_orig_vol_kg) as fcf_tot_orig_vol_kg,
        sum(fcf_tot_orig_vol_ul) as fcf_tot_orig_vol_ul,
        sum(ly_fcf_tot_vol_ca) as ly_fcf_tot_vol_ca,
        sum(ly_fcf_tot_vol_kg) as ly_fcf_tot_vol_kg,
        sum(ly_fcf_tot_vol_ul) as ly_fcf_tot_vol_ul
    from slsuber_forecast
    inner join snapshot_union
        on slsuber_forecast.snapshot_forecast_date = snapshot_union.snapshot
    group by
        source_system,
        source_item_identifier,
        calendar_date,
        source_content_filter,
        plan_source_customer_code,
        document_company,
        snapshot_forecast_date,
        cy_line_actual_ship_date,
        cy_scheduled_ship_date,
        line_status_code,
        frozen_forecast
),

cte_budget as (
    select
        source_system,
        source_item_identifier,
        calendar_date,
        source_content_filter,
        plan_source_customer_code,
        document_company as company,
        snapshot_forecast_date,
        cy_line_actual_ship_date,
        cy_scheduled_ship_date,
        line_status_code,
        frozen_forecast,
        sum(fcf_tot_vol_ca) as fcf_tot_vol_ca,
        sum(fcf_tot_vol_kg) as fcf_tot_vol_kg,
        sum(fcf_tot_vol_ul) as fcf_tot_vol_ul,
        sum(fcv_qty_ca_effective_base_fc_si) as fcf_base_vol_ca,
        sum(fcv_qty_kg_effective_base_fc_si) as fcf_base_vol_kg,
        sum(fcv_qty_ul_effective_base_fc_si) as fcf_base_vol_ul,
        sum(fcf_tot_orig_vol_ca) as fcf_tot_orig_vol_ca,
        sum(fcf_tot_orig_vol_kg) as fcf_tot_orig_vol_kg,
        sum(fcf_tot_orig_vol_ul) as fcf_tot_orig_vol_ul,
        sum(ly_fcf_tot_vol_ca) as ly_fcf_tot_vol_ca,
        sum(ly_fcf_tot_vol_kg) as ly_fcf_tot_vol_kg,
        sum(ly_fcf_tot_vol_ul) as ly_fcf_tot_vol_ul
    from slsuber_budget
    group by
        source_system,
        source_item_identifier,
        calendar_date,
        source_content_filter,
        plan_source_customer_code,
        document_company,
        snapshot_forecast_date,
        cy_line_actual_ship_date,
        cy_scheduled_ship_date,
        line_status_code,
        frozen_forecast
),

-- aggregation here is aggregating up from ship to to plan to
cte_actuals as (
    select
        source_system,
        source_item_identifier,
        calendar_date,
        source_content_filter,
        plan_source_customer_code,
        document_company as company,
        cy_line_actual_ship_date,
        cy_scheduled_ship_date,
        line_status_code,
        sum(cy_shipped_ca_quantity) as cy_shipped_ca_quantity,
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
    group by
        source_system,
        source_item_identifier,
        calendar_date,
        source_content_filter,
        plan_source_customer_code,
        document_company,
        cy_line_actual_ship_date,
        cy_scheduled_ship_date,
        line_status_code
),

-- full field list, null for actuals only fields
forecast_final as (
    select
        fcst.source_system,
        fcst.source_item_identifier,
        fcst.calendar_date,
        fcst.source_content_filter,
        fcst.plan_source_customer_code,
        fcst.company,
        fcst.snapshot_forecast_date,
        fcst.cy_line_actual_ship_date,
        fcst.cy_scheduled_ship_date,
        fcst.line_status_code,
        fcst.frozen_forecast,
        fcst.fcf_tot_vol_ca,
        fcst.fcf_tot_vol_ca * itm_ext.consumer_units as fcf_tot_vol_cu,
        fcst.fcf_tot_vol_kg,
        fcst.fcf_tot_vol_ca
        * itm_ext.consumer_units_in_trade_units as fcf_tot_vol_pk,
        fcst.fcf_tot_vol_ul,
        fcst.fcf_base_vol_ca,
        fcst.fcf_base_vol_ca * itm_ext.consumer_units as fcf_base_vol_cu,
        fcst.fcf_base_vol_kg,
        fcst.fcf_base_vol_ca
        * itm_ext.consumer_units_in_trade_units as fcf_base_vol_pk,
        fcst.fcf_base_vol_ul,
        fcst.fcf_tot_orig_vol_ca,
        fcst.fcf_tot_orig_vol_ca
        * itm_ext.consumer_units as fcf_tot_orig_vol_cu,
        fcst.fcf_tot_orig_vol_kg,
        fcst.fcf_tot_orig_vol_ca
        * itm_ext.consumer_units_in_trade_units as fcf_tot_orig_vol_pk,
        fcst.fcf_tot_orig_vol_ul,
        fcst.ly_fcf_tot_vol_ca,
        fcst.ly_fcf_tot_vol_ca * itm_ext.consumer_units as ly_fcf_tot_vol_cu,
        fcst.ly_fcf_tot_vol_kg,
        fcst.ly_fcf_tot_vol_ca
        * itm_ext.consumer_units_in_trade_units as ly_fcf_tot_vol_pk,
        fcst.ly_fcf_tot_vol_ul,
        null as cy_shipped_ca_quantity,
        null as cy_shipped_cu_quantity,
        null as cy_shipped_kg_quantity,
        null as cy_shipped_pk_quantity,
        null as cy_shipped_ul_quantity,
        null as cy_ordered_ca_quantity,
        null as cy_ordered_cu_quantity,
        null as cy_ordered_kg_quantity,
        null as cy_ordered_pk_quantity,
        null as cy_ordered_ul_quantity,
        null as ly_shipped_ca_quantity,
        null as ly_shipped_cu_quantity,
        null as ly_shipped_kg_quantity,
        null as ly_shipped_pk_quantity,
        null as ly_shipped_ul_quantity,
        null as ly_ordered_ca_quantity,
        null as ly_ordered_cu_quantity,
        null as ly_ordered_kg_quantity,
        null as ly_ordered_pk_quantity,
        null as ly_ordered_ul_quantity,
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
        dt.report_fiscal_year,
        dt.report_fiscal_year_period_no,
        plan_cust.market as market,
        plan_cust.sub_market as submarket,
        plan_cust.trade_class as trade_class,
        plan_cust.trade_group as trade_group,
        plan_cust.trade_type as trade_type,
        plan_cust.trade_sector_desc as trade_sector,
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
        dtp.planning_year_start_dt as planning_year_start_dt,
        dtp_sfd.planning_month_start_dt as sfd_planning_month_start_dt,
        dtp_sfd.planning_month_end_dt as sfd_planning_month_end_dt,
        dtp_sfd.planning_month_code as sfd_planning_month_code,
        scen.delineation_date as frozen_forecast_delineation_date,
        (case when prm.customer is null then '0' else '1' end) as isonpromo_flag
    from cte_forecast as fcst
    left join itm_ext
        on itm_ext.source_item_identifier = fcst.source_item_identifier
    left join cte_v_wtx_cust_planning as plan_cust
        on
            trim(fcst.plan_source_customer_code)
            = trim(plan_cust.trade_type_code)
            and fcst.company = plan_cust.company_code
    left join cte_planning_date_oc as dtp
        on
            fcst.source_system = dtp.source_system
            and fcst.calendar_date = dtp.calendar_date
    left join cte_planning_date_oc as dtp_sfd
        on
            fcst.source_system = dtp_sfd.source_system
            and fcst.snapshot_forecast_date = dtp_sfd.calendar_date
    left join cte_dim_date as dt
        on fcst.calendar_date = dt.calendar_date
    left join cte_sls_wtx_budget_scen_xref as scen
        on upper(trim(scen.frozen_forecast)) = upper(trim(fcst.frozen_forecast))
    left join cte_v_sls_wtx_onpromo as prm
        on
            prm.customer = trim(fcst.plan_source_customer_code)
            and prm.source_item_identifier = trim(fcst.source_item_identifier)
            and prm.date
            = dateadd(
                'DAY',
                -1,
                date_trunc('WEEK', dateadd('DAY', 1, fcst.calendar_date))
            )
),

budget_final as (
    select
        budget.source_system,
        budget.source_item_identifier,
        budget.calendar_date,
        budget.source_content_filter,
        budget.plan_source_customer_code,
        budget.company,
        budget.snapshot_forecast_date,
        budget.cy_line_actual_ship_date,
        budget.cy_scheduled_ship_date,
        budget.line_status_code,
        budget.frozen_forecast,
        budget.fcf_tot_vol_ca,
        budget.fcf_tot_vol_ca * itm_ext.consumer_units as fcf_tot_vol_cu,
        budget.fcf_tot_vol_kg,
        budget.fcf_tot_vol_ca
        * itm_ext.consumer_units_in_trade_units as fcf_tot_vol_pk,
        budget.fcf_tot_vol_ul,
        budget.fcf_base_vol_ca,
        budget.fcf_base_vol_ca * itm_ext.consumer_units as fcf_base_vol_cu,
        budget.fcf_base_vol_kg,
        budget.fcf_base_vol_ca
        * itm_ext.consumer_units_in_trade_units as fcf_base_vol_pk,
        budget.fcf_base_vol_ul,
        budget.fcf_tot_orig_vol_ca,
        budget.fcf_tot_orig_vol_ca
        * itm_ext.consumer_units as fcf_tot_orig_vol_cu,
        budget.fcf_tot_orig_vol_kg,
        budget.fcf_tot_orig_vol_ca
        * itm_ext.consumer_units_in_trade_units as fcf_tot_orig_vol_pk,
        budget.fcf_tot_orig_vol_ul,
        budget.ly_fcf_tot_vol_ca,
        budget.ly_fcf_tot_vol_ca * itm_ext.consumer_units as ly_fcf_tot_vol_cu,
        budget.ly_fcf_tot_vol_kg,
        budget.ly_fcf_tot_vol_ca
        * itm_ext.consumer_units_in_trade_units as ly_fcf_tot_vol_pk,
        budget.ly_fcf_tot_vol_ul,
        null as cy_shipped_ca_quantity,
        null as cy_shipped_cu_quantity,
        null as cy_shipped_kg_quantity,
        null as cy_shipped_pk_quantity,
        null as cy_shipped_ul_quantity,
        null as cy_ordered_ca_quantity,
        null as cy_ordered_cu_quantity,
        null as cy_ordered_kg_quantity,
        null as cy_ordered_pk_quantity,
        null as cy_ordered_ul_quantity,
        null as ly_shipped_ca_quantity,
        null as ly_shipped_cu_quantity,
        null as ly_shipped_kg_quantity,
        null as ly_shipped_pk_quantity,
        null as ly_shipped_ul_quantity,
        null as ly_ordered_ca_quantity,
        null as ly_ordered_cu_quantity,
        null as ly_ordered_kg_quantity,
        null as ly_ordered_pk_quantity,
        null as ly_ordered_ul_quantity,
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
        dt.report_fiscal_year,
        dt.report_fiscal_year_period_no,
        plan_cust.market as market,
        plan_cust.sub_market as submarket,
        plan_cust.trade_class as trade_class,
        plan_cust.trade_group as trade_group,
        plan_cust.trade_type as trade_type,
        plan_cust.trade_sector_desc as trade_sector,
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
        dtp.planning_year_start_dt as planning_year_start_dt,
        dtp_sfd.planning_month_start_dt as sfd_planning_month_start_dt,
        dtp_sfd.planning_month_end_dt as sfd_planning_month_end_dt,
        dtp_sfd.planning_month_code as sfd_planning_month_code,
        scen.delineation_date as frozen_forecast_delineation_date,
        (case when prm.customer is null then '0' else '1' end) as isonpromo_flag
    from cte_budget as budget
    left join itm_ext
        on itm_ext.source_item_identifier = budget.source_item_identifier
    left join cte_v_wtx_cust_planning as plan_cust
        on
            trim(budget.plan_source_customer_code)
            = trim(plan_cust.trade_type_code)
            and budget.company = plan_cust.company_code
    left join cte_planning_date_oc as dtp
        on
            budget.source_system = dtp.source_system
            and budget.calendar_date = dtp.calendar_date
    left join cte_planning_date_oc as dtp_sfd
        on
            budget.source_system = dtp_sfd.source_system
            and budget.snapshot_forecast_date = dtp_sfd.calendar_date
    left join cte_dim_date as dt
        on budget.calendar_date = dt.calendar_date
    left join cte_sls_wtx_budget_scen_xref as scen
        on
            upper(trim(scen.frozen_forecast))
            = upper(trim(budget.frozen_forecast))
    left join cte_v_sls_wtx_onpromo as prm
        on
            prm.customer = trim(budget.plan_source_customer_code)
            and prm.source_item_identifier = trim(budget.source_item_identifier)
            and prm.date
            = dateadd(
                'DAY',
                -1,
                date_trunc('WEEK', dateadd('DAY', 1, budget.calendar_date))
            )
),

-- full field list, null for forecast only fields
actuals_final as (
    select
        actuals.source_system,
        actuals.source_item_identifier,
        actuals.calendar_date,
        actuals.source_content_filter,
        actuals.plan_source_customer_code,
        actuals.company,
        null as snapshot_forecast_date,
        actuals.cy_line_actual_ship_date,
        actuals.cy_scheduled_ship_date,
        actuals.line_status_code,
        null as frozen_forecast,
        null as fcf_tot_vol_ca,
        null as fcf_tot_vol_cu,
        null as fcf_tot_vol_kg,
        null as fcf_tot_vol_pk,
        null as fcf_tot_vol_ul,
        null as fcf_base_vol_ca,
        null as fcf_base_vol_cu,
        null as fcf_base_vol_kg,
        null as fcf_base_vol_pk,
        null as fcf_base_vol_ul,
        null as fcf_tot_orig_vol_ca,
        null as fcf_tot_orig_vol_cu,
        null as fcf_tot_orig_vol_kg,
        null as fcf_tot_orig_vol_pk,
        null as fcf_tot_orig_vol_ul,
        null as ly_fcf_tot_vol_ca,
        null as ly_fcf_tot_vol_cu,
        null as ly_fcf_tot_vol_kg,
        null as ly_fcf_tot_vol_pk,
        null as ly_fcf_tot_vol_ul,
        actuals.cy_shipped_ca_quantity,
        actuals.cy_shipped_ca_quantity
        * itm_ext.consumer_units as cy_shipped_cu_quantity,
        actuals.cy_shipped_kg_quantity,
        actuals.cy_shipped_ca_quantity
        * itm_ext.consumer_units_in_trade_units as cy_shipped_pk_quantity,
        actuals.cy_shipped_ul_quantity,
        actuals.cy_ordered_ca_quantity,
        actuals.cy_ordered_ca_quantity
        * itm_ext.consumer_units as cy_ordered_cu_quantity,
        actuals.cy_ordered_kg_quantity,
        actuals.cy_ordered_ca_quantity
        * itm_ext.consumer_units_in_trade_units as cy_ordered_pk_quantity,
        actuals.cy_ordered_ul_quantity,
        actuals.ly_shipped_ca_quantity,
        actuals.ly_shipped_ca_quantity
        * itm_ext.consumer_units as ly_shipped_cu_quantity,
        actuals.ly_shipped_kg_quantity,
        actuals.ly_shipped_ca_quantity
        * itm_ext.consumer_units_in_trade_units as ly_shipped_pk_quantity,
        actuals.ly_shipped_ul_quantity,
        actuals.ly_ordered_ca_quantity,
        actuals.ly_ordered_ca_quantity
        * itm_ext.consumer_units as ly_ordered_cu_quantity,
        actuals.ly_ordered_kg_quantity,
        actuals.ly_ordered_ca_quantity
        * itm_ext.consumer_units_in_trade_units as ly_ordered_pk_quantity,
        actuals.ly_ordered_ul_quantity,
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
        dt.report_fiscal_year,
        dt.report_fiscal_year_period_no,
        plan_cust.market as market,
        plan_cust.sub_market as submarket,
        plan_cust.trade_class as trade_class,
        plan_cust.trade_group as trade_group,
        plan_cust.trade_type as trade_type,
        plan_cust.trade_sector_desc as trade_sector,
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
        dtp.planning_year_start_dt as planning_year_start_dt,
        null as sfd_planning_month_start_dt,
        null as sfd_planning_month_end_dt,
        null as sfd_planning_month_code,
        null as frozen_forecast_delineation_date,
        (case when prm.customer is null then '0' else '1' end) as isonpromo_flag
    from cte_actuals as actuals
    left join itm_ext
        on itm_ext.source_item_identifier = actuals.source_item_identifier
    left join cte_v_wtx_cust_planning as plan_cust
        on
            trim(actuals.plan_source_customer_code)
            = trim(plan_cust.trade_type_code)
            and actuals.company = plan_cust.company_code
    left join cte_planning_date_oc as dtp
        on
            actuals.source_system = dtp.source_system
            and actuals.calendar_date = dtp.calendar_date
    left join cte_dim_date as dt
        on actuals.calendar_date = dt.calendar_date
    left join cte_v_sls_wtx_onpromo as prm
        on
            prm.customer = trim(actuals.plan_source_customer_code)
            and prm.source_item_identifier
            = trim(actuals.source_item_identifier)
            and prm.date
            = dateadd(
                'DAY',
                -1,
                date_trunc('WEEK', dateadd('DAY', 1, actuals.calendar_date))
            )

)

select * from forecast_final
union
select * from budget_final
union
select * from actuals_final
