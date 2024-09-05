{{
    config(
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        tags=["sales", "performance", "sales_performance_lite"],
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        on_schema_change="sync_all_columns",
        pre_hook="""
                                    {{ truncate_if_exists(this.schema, this.table) }}""",
    )
}}

with
    dim_date_cte as (select * from {{ ref("src_dim_date") }}),
    uber as (
        select *
        from {{ ref("fct_wbx_sls_uber") }}
        where
            snapshot_forecast_date in (
                select distinct calendar_date
                from dim_date_cte
                where
                    calendar_date in (
                        select to_date(calendar_week_end_dt) as current_week_dt
                        from dim_date_cte
                        where calendar_date in (current_date - 14, current_date - 7)
                    )
            )
            or snapshot_forecast_date is null
    ),
    item_master_ext as (select * from {{ ref("dim_wbx_item_ext") }}),
    cust_master_ext as (select * from {{ ref("dim_wbx_customer_ext") }}),
    dim_date_oc_cte as (select * from {{ ref("dim_wbx_date_oc") }}),
    dim_planning_oc as (select * from {{ ref("dim_wbx_planning_date_oc") }}),
    scen_xref as (select * from {{ ref("src_sls_wtx_budget_scen_xref") }}),
    cust_planning as (select * from {{ ref("dim_wbx_cust_planning") }}),
    uber_budget as (select * from {{ ref("int_f_wbx_sls_performance_budget") }}),
    uber_current_week as (
        select * from {{ ref("int_f_wbx_sls_performance_current_week") }}
    ),
    uber_last_week as (select * from {{ ref("int_f_wbx_sls_performance_last_week") }}),
    uber_le as (select * from {{ ref("int_f_wbx_sls_performance_le") }}),

    pre_processing as (
        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "itm_ext.branding_desc",
                        "itm_ext.product_class_desc",
                        "itm_ext.sub_product_desc",
                        "itm_ext.pack_size_desc",
                        "coalesce(cust_ext.market_desc, plan.market, '-')",
                        "coalesce(cust_ext.sub_market_desc, plan.sub_market, '-')",
                        "coalesce(cust_ext.trade_class_desc, plan.trade_class, '-')",
                        "coalesce(cust_ext.trade_group_desc, plan.trade_group, '-')",
                        "coalesce(cust_ext.trade_sector_desc, plan.trade_sector_desc, '-')",
                        "coalesce(cust_ext.trade_type_desc, plan.trade_type,'-')",
                        "itm_ext.dummy_product_flag",
                        "ub.calendar_date",
                        "dt.report_fiscal_year",
                        "dt.report_fiscal_year_period_no",
                        "ub.snapshot_forecast_date",
                        "ub.frozen_forecast",
                        "scen.delineation_date",
                    ]
                )
            }}
            as unique_key,
            itm_ext.branding_desc as "Branding",
            itm_ext.product_class_desc as "Product Class",
            itm_ext.sub_product_desc as "Sub Product",
            itm_ext.pack_size_desc as "Packaging Size",
            coalesce(cust_ext.market_desc, plan.market, '-') as market,
            coalesce(cust_ext.sub_market_desc, plan.sub_market, '-') as submarket,
            coalesce(cust_ext.trade_class_desc, plan.trade_class, '-') as trade_class,
            coalesce(cust_ext.trade_group_desc, plan.trade_group, '-') as trade_group,
            coalesce(cust_ext.trade_type_desc, plan.trade_type, '-') as trade_type,
            coalesce(
                cust_ext.trade_sector_desc, plan.trade_sector_desc, '-'
            ) as trade_sector,
            nvl(itm_ext.dummy_product_flag, 0) as "Budget PCOS Var Flag",
            ub.calendar_date as "calendar date",
            dt.report_fiscal_year as "Report fiscal year",
            dt.report_fiscal_year_period_no as "Report fiscal year period no",
            ub.snapshot_forecast_date as "Snapshot forecast date",
            ub.frozen_forecast as "Frozen forecast",
            scen.delineation_date as "Frozen forecast delineation date",
            sum(
                nvl(ub.fcf_ap_fixed_trade_cust_invoiced, 0)
            ) as "Forecast Customer-Invoiced Fixed Trade £",
            sum(nvl(ub.fcf_ap_variable_trade, 0)) as "Forecast Total Variable Trade £",
            sum(nvl(ub.cy_base_ext_boughtin_amt, 0)) as "Cy Std Bought-In Amt £",
            sum(nvl(ub.cy_base_ext_copack_amt, 0)) as "Cy Std Co-Packing Amt £",
            sum(nvl(ub.cy_base_ext_ing_amt, 0)) as "Cy Std Ingredient Amt £",
            sum(nvl(ub.cy_base_ext_lbr_amt, 0)) as "Cy Std Labour Amt £",
            sum(nvl(ub.cy_base_ext_oth_amt, 0)) as "Cy Std Other Amt £",
            sum(nvl(ub.cy_base_ext_pkg_amt, 0)) as "Cy Std Packaging Amt £",
            sum(
                nvl(ub.cy_base_pcos_boughtin_var, 0)
            ) as "Cy Var to Std Bought-In Amt £",
            sum(nvl(ub.cy_base_pcos_copack_var, 0)) as "Cy Var to Std Co-Packing Amt £",
            sum(nvl(ub.cy_base_pcos_raw_var, 0)) as "Cy Var to Std Ingredients Amt £",
            sum(nvl(ub.cy_base_pcos_labour_var, 0)) as "Cy Var to Std Labour Amt £",
            sum(nvl(ub.cy_base_pcos_other_var, 0)) as "Cy Var to Std Other Amt £",
            sum(nvl(ub.cy_base_pcos_pack_var, 0)) as "Cy Var to Std Packaging Amt £",
            sum(nvl(ub.cy_shipped_ca_quantity, 0)) as "Cy Despatched Case Quantity",
            sum(
                nvl(ub.cy_shipped_ca_quantity, 0) * itm_ext.consumer_units
            ) as "CY Despatched Consumer Unit Quantity",
            sum(nvl(ub.cy_shipped_kg_quantity, 0)) as "Cy Despatched Kg Quantity",
            sum(
                nvl(ub.cy_shipped_ca_quantity, 0)
                * itm_ext.consumer_units_in_trade_units
            ) as "CY Despatched Packet Quantity",
            sum(nvl(ub.cy_shipped_ul_quantity, 0)) as "Cy Despatched Pallet Quantity",
            sum(nvl(ub.ly_shipped_ca_quantity, 0)) as "Ly Despatched Case Quantity",
            sum(
                nvl(ub.ly_shipped_ca_quantity, 0) * itm_ext.consumer_units
            ) as "LY Despatched Consumer Unit Quantity",
            sum(nvl(ub.ly_shipped_kg_quantity, 0)) as "Ly Despatched Kg Quantity",
            sum(
                nvl(ub.ly_shipped_ca_quantity, 0)
                * itm_ext.consumer_units_in_trade_units
            ) as "LY Despatched Packet Quantity",
            sum(nvl(ub.ly_shipped_ul_quantity, 0)) as "Ly Despatched Pallet Quantity",
            sum(nvl(ub.cy_base_rpt_grs_amt, 0)) as "Cy Gross Amount £",
            sum(nvl(ub.cy_base_invoice_grs_amt, 0)) as "Cy Invoice Gross Amount £",
            sum(nvl(ub.ly_base_rpt_grs_amt, 0)) as "Ly Gross Amount £",
            sum(nvl(ub.cy_gl_base_trade_cat, 0)) as "Cy Category £",
            sum(nvl(ub.cy_gl_base_trade_drct_shp, 0)) as "Cy Direct Shopper Marking £",
            sum(
                nvl(ub.cy_gl_base_permd_csh_disc, 0)
            ) as "Cy Early Settlement Discount £",
            sum(nvl(ub.cy_gl_base_permd_edlp, 0)) as "Cy EDLP £",
            sum(nvl(ub.cy_gl_base_trade_ag_cst, 0)) as "Cy Field Marketing £",
            sum(nvl(ub.cy_gl_base_trade_fxd_pymt, 0)) as "Cy Fixed Annual Pymts £",
            sum(nvl(ub.cy_gl_base_permd_rsa_inct, 0)) as "Cy Growth Incentives £",
            sum(
                nvl(ub.cy_gl_base_trade_indrct_shp, 0)
            ) as "Cy Indirect Shopper Marking £",
            sum(nvl(ub.cy_gl_base_trade_oth_drct_pymt, 0)) as "Cy Other Direct Pymts £",
            sum(
                nvl(ub.cy_gl_base_trade_oth_indrct_pymt, 0)
            ) as "Cy Other Indirect Pymts £",
            sum(nvl(ub.cy_gl_base_trade_other, 0)) as "Cy Other Trade £",
            sum(nvl(ub.cy_gl_base_trade_promo_fixed, 0)) as "Cy Promo Fixed Funding £",
            sum(nvl(ub.cy_gl_base_permd_rng_spt, 0)) as "Cy Range Support Allowance £",
            sum(nvl(ub.cy_gl_base_trade_retro, 0)) as "Cy Retro £",
            sum(nvl(ub.fcf_ap_added_value_pack, 0)) as "Forecast Added Value Pack £",
            sum(
                nvl(ub.fcf_ap_early_settlement_disc, 0)
            ) as "Forecast Early Settlement Discount £",
            sum(nvl(ub.fcf_ap_everyday_low_prices, 0)) as "Forecast EDLP £",
            sum(nvl(ub.fcf_ap_gross_selling_value, 0)) as "Forecast Gross Amount £",
            sum(nvl(ub.fcf_ap_growth_incentives, 0)) as "Forecast Growth Incentives £",
            sum(nvl(ub.fcf_ap_net_sales_value, 0)) as "Forecast Net Sales £",
            sum(nvl(ub.fcf_ap_tot_prime_cost_standard, 0)) as "Forecast PCOS (Std) £",
            sum(nvl(ub.fcf_ap_tot_prime_cost_variance, 0)) as "Forecast PCOS (Var) £",
            sum(nvl(ub.fcf_ap_permanent_disc, 0)) as "Forecast Range Support £",
            sum(
                nvl(ub.fcf_ap_range_support_incentives, 0)
            ) as "Forecast RSA Incentives £",
            sum(
                nvl(ub.fcf_ap_total_trade_cust_invoiced, 0)
            ) as "Forecast Total Customer-Invoiced Trade £",
            sum(
                nvl(ub.fcf_ap_fixed_trade_non_cust_invoiced, 0)
            ) as "Forecast Total Non-Customer Invoiced Fixed Trade £",
            sum(nvl(ub.fcf_tot_vol_ca, 0)) as "Forecast Total Volume Case",
            sum(
                nvl(ub.fcf_tot_vol_ca, 0) * itm_ext.consumer_units
            ) as "Forecast Total Volume Consumer Units",
            sum(nvl(ub.fcf_tot_vol_kg, 0)) as "Forecast Total Volume Kg",
            sum(
                nvl(ub.fcf_tot_vol_ca, 0) * itm_ext.consumer_units_in_trade_units
            ) as "Forecast Total Volume Packet",
            sum(nvl(ub.fcf_tot_vol_ul, 0)) as "Forecast Total Volume Pallet",
            sum(nvl(ub.budget_amount, 0)) as "Budget Gross Amount £",
            sum(nvl(ub.ly_base_ext_boughtin_amt, 0)) as "Ly Std Bought-In Amt £",
            sum(nvl(ub.ly_base_ext_copack_amt, 0)) as "Ly Std Co-Packing Amt £",
            sum(nvl(ub.ly_base_ext_ing_amt, 0)) as "Ly Std Ingredient Amt £",
            sum(nvl(ub.ly_base_ext_lbr_amt, 0)) as "Ly Std Labour Amt £",
            sum(nvl(ub.ly_base_ext_oth_amt, 0)) as "Ly Std Other Amt £",
            sum(nvl(ub.ly_base_ext_pkg_amt, 0)) as "Ly Std Packaging Amt £",
            sum(
                nvl(ub.ly_base_pcos_boughtin_var, 0)
            ) as "Ly Var to Std Bought-In Amt £",
            sum(
                nvl(ub.ly_base_pcos_copack_var, 0)
            ) as "Ly Var to Std Co-Packing Amt  £",
            sum(nvl(ub.ly_base_pcos_raw_var, 0)) as "Ly Var to Std Ingredients Amt  £",
            sum(nvl(ub.ly_base_pcos_labour_var, 0)) as "Ly Var to Std Labour Amt  £",
            sum(nvl(ub.ly_base_pcos_other_var, 0)) as "Ly Var to Std Other Amt  £",
            sum(nvl(ub.ly_base_pcos_pack_var, 0)) as "Ly Var to Std Packaging Amt  £",
            sum(nvl(ub.ly_base_invoice_grs_amt, 0)) as "Ly Invoice Gross Amount £",
            sum(nvl(ub.cy_gl_base_trade_avp, 0)) as "Cy AVP Discount £",
            sum(nvl(ub.ly_gl_base_trade_avp, 0)) as "Ly AVP Discount £",
            sum(nvl(ub.ly_gl_base_trade_cat, 0)) as "Ly Category £",
            sum(nvl(ub.ly_gl_base_trade_drct_shp, 0)) as "Ly Direct Shopper Marking £",
            sum(
                nvl(ub.ly_gl_base_permd_csh_disc, 0)
            ) as "Ly Early Settlement Discount £",
            sum(nvl(ub.ly_gl_base_permd_edlp, 0)) as "Ly EDLP £",
            sum(nvl(ub.ly_gl_base_trade_ag_cst, 0)) as "Ly Field Marketing £",
            sum(nvl(ub.ly_gl_base_trade_fxd_pymt, 0)) as "Ly Fixed Annual Pymts £",
            sum(nvl(ub.ly_gl_base_permd_rsa_inct, 0)) as "Ly Growth Incentives £",
            sum(
                nvl(ub.ly_gl_base_trade_indrct_shp, 0)
            ) as "Ly Indirect Shopper Marking £",
            sum(nvl(ub.ly_gl_base_trade_oth_drct_pymt, 0)) as "Ly Other Direct Pymts £",
            sum(
                nvl(ub.ly_gl_base_trade_oth_indrct_pymt, 0)
            ) as "Ly Other Indirect Pymts £",
            sum(nvl(ub.ly_gl_base_trade_other, 0)) as "Ly Other Trade £",
            sum(nvl(ub.ly_gl_base_trade_promo_fixed, 0)) as "Ly Promo Fixed Funding £",
            sum(nvl(ub.ly_gl_base_permd_rng_spt, 0)) as "Ly Range Support Allowance £",
            sum(nvl(ub.ly_gl_base_trade_retro, 0)) as "Ly Retro £",
            sum(
                nvl(ub.cy_base_invoice_grs_amt, 0)
                + nvl(ub.fcf_ap_gross_selling_value, 0)
            ) as "(Budget) Forecast Gross Amount (Combo)",
            sum(
                nvl(ub.cy_base_invoice_grs_amt, 0)
                + nvl(ub.fcf_ap_gross_selling_value, 0)
                + nvl(((ub.cy_gl_base_trade_avp * -1) + ub.fcf_ap_added_value_pack), 0)
            ) as "(Budget) Forecast Gross Sales £ (Combo)",

            sum(nvl(ub.fcf_ap_variable_trade, 0)) as "Budget Variable Trade Spend",
            sum(nvl(ub.cy_gl_base_trade_avp, 0) * -1) as "CY Added Value Pack £",
            sum(
                ifnull(cy_gl_base_trade_promo_fixed, 0)
                + ifnull(cy_gl_base_trade_fxd_pymt, 0)
                + ifnull(cy_gl_base_trade_drct_shp, 0)
                + ifnull(cy_gl_base_trade_oth_drct_pymt, 0)
            ) as "CY Customer-Invoiced Fixed Trade £",
            sum(
                ifnull(cy_base_invoice_grs_amt, 0)
                + ifnull(cy_gl_base_trade_avp * -1, 0)
            ) as "CY Gross Sales £",
            sum(
                nvl(ub.cy_base_ext_boughtin_amt, 0)
                + nvl(ub.cy_base_ext_copack_amt, 0)
                + nvl(ub.cy_base_ext_ing_amt, 0)
                + nvl(ub.cy_base_ext_lbr_amt, 0)
                + nvl(ub.cy_base_ext_oth_amt, 0)
                + nvl(ub.cy_base_ext_pkg_amt, 0)
            ) as "CY Total Std PCOS Amt £",
            sum(
                nvl(
                    nvl(ub.cy_base_pcos_boughtin_var, 0)
                    + nvl(ub.cy_base_pcos_copack_var, 0)
                    + nvl(ub.cy_base_pcos_raw_var, 0)
                    + nvl(ub.cy_base_pcos_labour_var, 0)
                    + nvl(ub.cy_base_pcos_other_var, 0)
                    + nvl(ub.cy_base_pcos_pack_var, 0),
                    0
                )
            ) as "CY Total Var to Std PCOS Amt £",
            sum(
                ifnull(cy_gl_base_permd_rng_spt, 0)
                + ifnull(cy_gl_base_permd_edlp, 0)
                + ifnull(cy_gl_base_permd_csh_disc, 0)
                + ifnull(cy_gl_base_permd_rsa_inct, 0)
            ) as "CY Permanent Discounts £",
            sum(
                ifnull(cy_gl_base_trade_indrct_shp, 0)
                + ifnull(cy_gl_base_trade_cat, 0)
                + ifnull(cy_gl_base_trade_oth_indrct_pymt, 0)
                + ifnull(cy_gl_base_trade_ag_cst, 0)
                + ifnull(cy_gl_base_trade_other, 0)
            ) as "CY Total Non-Customer Invoiced Fixed Trade £",
            sum(
                ifnull(cy_gl_base_trade_retro, 0) + ifnull(cy_gl_base_trade_avp, 0)
            ) as "CY Total Variable Trade £",
            sum(
                ifnull(cy_base_invoice_grs_amt + nvl(fcf_ap_gross_selling_value, 0), 0)
            ) as "Forecast 1 Gross Amount £ (Combo)",
            sum(
                ifnull(fcf_ap_permanent_disc, 0)
                + ifnull(fcf_ap_everyday_low_prices, 0)
                + ifnull(fcf_ap_early_settlement_disc, 0)
                + ifnull(fcf_ap_range_support_incentives, 0)
                + ifnull(fcf_ap_growth_incentives, 0)
            ) as "Forecast Permanent Discounts £",
            sum(
                case
                    when nvl(itm_ext.dummy_product_flag, 0) = 0
                    then
                        ifnull(fcf_ap_tot_prime_cost_standard_bought_in, 0)
                        + ifnull(fcf_ap_tot_prime_cost_standard_raw, 0)
                        + ifnull(fcf_ap_tot_prime_cost_standard_labour, 0)
                        + ifnull(fcf_ap_tot_prime_cost_standard_other, 0)
                        + ifnull(fcf_ap_tot_prime_cost_standard_packaging, 0)
                        + ifnull(fcf_ap_tot_prime_cost_standard_co_pack, 0)
                end
            ) as "Forecast Total Standard PCOS Amt £",
            sum(
                case
                    when nvl(itm_ext.dummy_product_flag, 0) = 1
                    then 1 * (ifnull(fcf_ap_tot_prime_cost_variance, 0))
                end
            ) as "Forecast Total Var PCOS Amt £",

            sum(
                ifnull(ly_base_invoice_grs_amt, 0)
                + ifnull(ly_gl_base_trade_avp * -1, 0)
            ) as "LY Gross Sales £",
            sum(
                ifnull(ly_gl_base_permd_rng_spt, 0)
                + ifnull(ly_gl_base_permd_edlp, 0)
                + ifnull(ly_gl_base_permd_csh_disc, 0)
                + ifnull(ly_gl_base_permd_rsa_inct, 0)
            ) as "LY Permanent Discounts £",
            sum(
                ifnull(
                    ifnull(ly_gl_base_trade_retro, 0) + ifnull(ly_gl_base_trade_avp, 0),
                    0
                ) + ifnull(
                    (
                        ifnull(ly_gl_base_trade_promo_fixed, 0)
                        + ifnull(ly_gl_base_trade_fxd_pymt, 0)
                        + ifnull(ly_gl_base_trade_drct_shp, 0)
                        + ifnull(ly_gl_base_trade_oth_drct_pymt, 0)
                    ),
                    0
                )
            ) as "LY Total Customer-Invoiced Trade £",
            sum(
                ifnull(ly_gl_base_trade_indrct_shp, 0)
                + ifnull(ly_gl_base_trade_cat, 0)
                + ifnull(ly_gl_base_trade_oth_indrct_pymt, 0)
                + ifnull(ly_gl_base_trade_ag_cst, 0)
                + ifnull(ly_gl_base_trade_other, 0)
            ) as "LY Total Non-Customer Invoiced Fixed Trade £",
            sum(
                ifnull(ly_base_ext_boughtin_amt, 0)
                + ifnull(ly_base_ext_ing_amt, 0)
                + ifnull(ly_base_ext_lbr_amt, 0)
                + ifnull(ly_base_ext_oth_amt, 0)
                + ifnull(ly_base_ext_pkg_amt, 0)
                + ifnull(ly_base_ext_copack_amt, 0)
            ) as "LY Total Std PCOS Amt £",
            sum(
                ifnull(ly_base_pcos_raw_var, 0)
                + ifnull(ly_base_pcos_copack_var, 0)
                + ifnull(ly_base_pcos_labour_var, 0)
                + ifnull(ly_base_pcos_other_var, 0)
                + ifnull(ly_base_pcos_pack_var, 0)
                + ifnull(ly_base_pcos_boughtin_var, 0)
            ) as "LY Total Var to Std PCOS Amt £",
            sum(
                ifnull(ly_gl_base_trade_retro, 0) + ifnull(ly_gl_base_trade_avp, 0)
            ) as "LY Total Variable Trade £",
            sum(
                (
                    ifnull(ly_gl_base_trade_promo_fixed, 0)
                    + ifnull(ly_gl_base_trade_fxd_pymt, 0)
                    + ifnull(ly_gl_base_trade_drct_shp, 0)
                    + ifnull(ly_gl_base_trade_oth_drct_pymt, 0)
                )
            ) as "LY Fixed Trade Customer-Invoiced (Combo Split)",
            sum(
                nvl(
                    nvl(ub.cy_base_ext_boughtin_amt, 0)
                    + nvl(ub.cy_base_ext_copack_amt, 0)
                    + nvl(ub.cy_base_ext_ing_amt, 0)
                    + nvl(ub.cy_base_ext_lbr_amt, 0)
                    + nvl(ub.cy_base_ext_oth_amt, 0)
                    + nvl(ub.cy_base_ext_pkg_amt, 0),
                    0
                ) + nvl(
                    nvl(ub.cy_base_pcos_boughtin_var, 0)
                    + nvl(ub.cy_base_pcos_copack_var, 0)
                    + nvl(ub.cy_base_pcos_raw_var, 0)
                    + nvl(ub.cy_base_pcos_labour_var, 0)
                    + nvl(ub.cy_base_pcos_other_var, 0)
                    + nvl(ub.cy_base_pcos_pack_var, 0),
                    0
                )
            ) as "CY Total PCOS Amt £",
            sum(
                ifnull(cy_gl_base_trade_retro, 0)
                + ifnull(cy_gl_base_trade_avp, 0)
                + (
                    ifnull(cy_gl_base_trade_promo_fixed, 0)
                    + ifnull(cy_gl_base_trade_fxd_pymt, 0)
                    + ifnull(cy_gl_base_trade_drct_shp, 0)
                    + ifnull(cy_gl_base_trade_oth_drct_pymt, 0)
                )
            ) as "CY Total Customer-Invoiced Trade £"

        from uber ub
        left join
            (
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
                from item_master_ext
                group by source_system, source_item_identifier
            ) itm_ext
            on ub.source_system = itm_ext.source_system
            and ub.source_item_identifier = itm_ext.source_item_identifier
        left join
            cust_master_ext cust_ext
            on ub.customer_addr_number_guid = cust_ext.customer_address_number_guid
            --and nvl(ub.document_company, 'WBX') = cust_ext.company_code
        left join
            cust_planning plan
            on trim(ub.plan_source_customer_code) = trim(plan.trade_type_code)
            and ub.document_company = plan.company_code
        left join
            (select * from scen_xref where not contains(frozen_forecast, 'TEST')) scen
            on upper(trim(scen.frozen_forecast)) = upper(trim(ub.frozen_forecast))
        left join dim_date_cte dt on ub.calendar_date = dt.calendar_date
        left join
            dim_planning_oc dtp_or
            on ub.source_system = dtp_or.source_system
            and coalesce(
                ub.trans_line_requested_date,
                ub.cy_scheduled_ship_date,
                ub.calendar_date
            )
            = dtp_or.calendar_date
        left join
            dim_planning_oc dtp_sfd
            on ub.source_system = dtp_sfd.source_system
            and ub.snapshot_forecast_date = dtp_sfd.calendar_date
        left join
            dim_date_oc_cte dd_oc
            on ub.source_system = dd_oc.source_system
            and ub.calendar_date = dd_oc.calendar_date
        where
            (nvl(ub.document_company, '-')) in ('WBX', '-')
            and coalesce(cust_ext.trade_type_desc, plan.trade_type, '-') <> 'BULK'
            and nvl(ub.source_content_filter, '-') not in ('EPOS', 'TERMS', '-')
            and ub.calendar_date
            >= dateadd('month', -27, date_trunc('year', current_date))
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
    ),

    final as (
        select
            main.*,
            uber_budget."Budget (Budget) Forecast Added Value Pack (Combo)",
            uber_budget."Budget (Budget) Forecast Gross Amount (Combo)",
            uber_budget."Budget (Budget) Forecast Gross Sales £ (Combo)",
            uber_budget."Budget CY Added Value Pack £",
            uber_budget."Budget CY Total Customer-Invoiced Trade Spend (Combo)",
            uber_budget."Budget (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
            uber_budget."Budget Cy PCOS Spend (Combo) Std",
            uber_budget."Budget Cy PCOS Spend (Combo) Act",
            uber_budget."Budget (Budget) Forecast Net Sales £ (Combo)",
            uber_budget."Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_budget."Budget (Budget) Forecast NRR £ (Combo)",
            uber_budget."Budget (Budget) Forecast Permanent Discounts £ (Combo)",
            uber_budget."Budget (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
            uber_budget."Budget (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
            uber_budget."Budget Budget Variable Trade £ (Combo)",
            uber_budget."Budget CY Customer-Invoiced Variable Trade £ (Combo Split)",
            uber_budget."Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_budget."Budget CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
            uber_budget."Budget CY Total Customer-Invoiced Trade (Combo)",
            uber_budget."Budget CY Total Variable Trade £ (Combo)",
            uber_budget."Budget Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_budget."Budget Forecast PCOS £ (Combo) Act",
            uber_budget."Budget Forecast PCOS £ (Combo) Std",
            uber_budget."Budget Forecast Permanent Discounts £ (Combo)",
            uber_budget."Budget (Budget) Forecast Gross Margin £ Act",
            uber_budget."Budget (Budget) Forecast Gross Margin £ Std",
            uber_budget."Budget (Budget) Forecast Total PCOS Amt £ Std PCOS",
            uber_budget."Budget Budget Fixed Trade Spend",
            uber_budget."Budget Budget PCOS Spend Act",
            uber_budget."Budget Budget PCOS Spend Std",
            uber_budget."Budget Customer-Invoiced Variable Trade % vs Budget (Split)",
            uber_budget."Budget Customer-Invoiced Variable Trade % vs Forecast (Split)",
            uber_budget."Budget Customer-Invoiced Variable Trade % vs LY (Split)",
            uber_budget."Budget Customer-Invoiced Variable Trade Var to Budget (Split)",
            uber_budget."Budget Customer-Invoiced Variable Trade Var to Forecast (Split)",
            uber_budget."Budget Customer-Invoiced Variable Trade Var to LY (Split)",
            uber_budget."Budget CY NRR £ (Combo)",
            uber_budget."Budget Cy Variable Trade Spend",
            uber_budget."Budget Forecast 1 Permanent Discounts £ (Combo)",
            uber_budget."Budget Forecast Added Value Pack (Combo)",
            uber_budget."Budget Forecast Fixed Trade £ (Combo)",
            uber_budget."Budget Forecast Fixed Trade Customer-Invoiced",
            uber_budget."Budget Forecast Fixed Trade Spend",
            uber_budget."Budget Forecast GCAT £ (Combo) Act",
            uber_budget."Budget Forecast GCAT £ (Combo) Std",
            uber_budget."Budget Forecast Gross Amount (Combo)",
            uber_budget."Budget Forecast Net Sales £ (Combo)",
            uber_budget."Budget Forecast Total Customer-Invoiced Trade £ (Combo)",
            uber_budget."Budget Forecast Total Customer-Invoiced Trade Spend (Combo)",
            uber_budget."Budget Forecast Variable Trade £ (Combo) (1)",
            uber_budget."Budget Forecast Variable Trade Spend",
            uber_budget."Budget LY NRR £ (Combo)",
            uber_budget."Budget LY Total Customer-Invoiced Trade (Combo)",
            uber_budget."Budget Abs Var Forecast Cust-Inv Trade",
            uber_budget."Budget Abs Var Forecast GCAT Std",
            uber_budget."Budget Abs Var Forecast NRR",
            uber_budget."Budget Abs Var Forecast GCAT Act",
            uber_budget."Budget CY GCAT £ (Combo) Act",
            uber_budget."Budget CY GCAT £ (Combo) Std",
            uber_budget."Budget Forecast NRR £ (Combo)",

            uber_le."LE (Budget) Forecast Added Value Pack (Combo)",
            uber_le."LE Budget (Budget) Forecast Gross Amount (Combo)",
            uber_le."LE (Budget) Forecast Gross Sales £ (Combo)",
            uber_le."LE CY Added Value Pack £",
            uber_le."LE CY Total Customer-Invoiced Trade Spend (Combo)",
            uber_le."LE (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
            uber_le."LE Cy PCOS Spend (Combo) Std",
            uber_le."LE Cy PCOS Spend (Combo) Act",
            uber_le."LE (Budget) Forecast Net Sales £ (Combo)",
            uber_le."LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_le."LE (Budget) Forecast NRR £ (Combo)",
            uber_le."LE (Budget) Forecast Permanent Discounts £ (Combo)",
            uber_le."LE (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
            uber_le."LE (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
            uber_le."LE Budget Variable Trade £ (Combo)",
            uber_le."LE CY Customer-Invoiced Variable Trade £ (Combo Split)",
            uber_le."LE CY Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_le."LE CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
            uber_le."LE CY Total Customer-Invoiced Trade (Combo)",
            uber_le."LE CY Total Variable Trade £ (Combo)",
            uber_le."LE Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_le."LE Forecast PCOS £ (Combo) Act",
            uber_le."LE Forecast PCOS £ (Combo) Std",
            uber_le."LE Forecast Permanent Discounts £ (Combo)",
            uber_le."LE (Budget) Forecast Gross Margin £ Act",
            uber_le."LE (Budget) Forecast Gross Margin £ Std",
            uber_le."LE (Budget) Forecast Total PCOS Amt £ Std PCOS",
            uber_le."LE Budget Fixed Trade Spend",
            uber_le."LE Budget PCOS Spend Act",
            uber_le."LE Budget PCOS Spend Std",
            uber_le."LE Customer-Invoiced Variable Trade % vs Budget (Split)",
            uber_le."LE Customer-Invoiced Variable Trade % vs Forecast (Split)",
            uber_le."LE Customer-Invoiced Variable Trade % vs LY (Split)",
            uber_le."LE Customer-Invoiced Variable Trade Var to Budget (Split)",
            uber_le."LE Customer-Invoiced Variable Trade Var to Forecast (Split)",
            uber_le."LE Customer-Invoiced Variable Trade Var to LY (Split)",
            uber_le."LE CY NRR £ (Combo)",
            uber_le."LE Cy Variable Trade Spend",
            uber_le."LE Forecast 1 Permanent Discounts £ (Combo)",
            uber_le."LE Forecast Added Value Pack (Combo)",
            uber_le."LE Forecast Fixed Trade £ (Combo)",
            uber_le."LE Forecast Fixed Trade Customer-Invoiced",
            uber_le."LE Forecast Fixed Trade Spend",
            uber_le."LE Forecast GCAT £ (Combo) Act",
            uber_le."LE Forecast GCAT £ (Combo) Std",
            uber_le."LE Forecast Gross Amount (Combo)",
            uber_le."LE Forecast Net Sales £ (Combo)",
            uber_le."LE Forecast Total Customer-Invoiced Trade £ (Combo)",
            uber_le."LE Forecast Total Customer-Invoiced Trade Spend (Combo)",
            uber_le."LE Forecast Variable Trade £ (Combo) (1)",
            uber_le."LE Forecast Variable Trade Spend",
            uber_le."LE LY NRR £ (Combo)",
            uber_le."LE LY Total Customer-Invoiced Trade (Combo)",
            uber_le."LE Abs Var Forecast Cust-Inv Trade",
            uber_le."LE Abs Var Forecast GCAT Std",
            uber_le."LE Abs Var Forecast NRR",
            uber_le."LE Abs Var Forecast GCAT Act",
            uber_le."LE CY GCAT £ (Combo) Act",
            uber_le."LE CY GCAT £ (Combo) Std",
            uber_le."LE Forecast NRR £ (Combo)",

            uber_last_week."LW (Budget) Forecast Added Value Pack (Combo)",
            uber_last_week."LW Budget (Budget) Forecast Gross Amount (Combo)",
            uber_last_week."LW (Budget) Forecast Gross Sales £ (Combo)",
            uber_last_week."LW CY Added Value Pack £",
            uber_last_week."LW CY Total Customer-Invoiced Trade Spend (Combo)",
            uber_last_week."LW (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
            uber_last_week."LW Cy PCOS Spend (Combo) Std",
            uber_last_week."LW Cy PCOS Spend (Combo) Act",
            uber_last_week."LW (Budget) Forecast Net Sales £ (Combo)",
            uber_last_week."LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_last_week."LW (Budget) Forecast NRR £ (Combo)",
            uber_last_week."LW (Budget) Forecast Permanent Discounts £ (Combo)",
            uber_last_week."LW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
            uber_last_week."LW (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
            uber_last_week."LW Budget Variable Trade £ (Combo)",
            uber_last_week."LW CY Customer-Invoiced Variable Trade £ (Combo Split)",
            uber_last_week."LW CY Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_last_week."LW CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
            uber_last_week."LW CY Total Customer-Invoiced Trade (Combo)",
            uber_last_week."LW CY Total Variable Trade £ (Combo)",
            uber_last_week."LW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_last_week."LW Forecast PCOS £ (Combo) Act",
            uber_last_week."LW Forecast PCOS £ (Combo) Std",
            uber_last_week."LW Forecast Permanent Discounts £ (Combo)",
            uber_last_week."LW (Budget) Forecast Gross Margin £ Act",
            uber_last_week."LW (Budget) Forecast Gross Margin £ Std",
            uber_last_week."LW (Budget) Forecast Total PCOS Amt £ Std PCOS",
            uber_last_week."LW Budget Fixed Trade Spend",
            uber_last_week."LW Budget PCOS Spend Act",
            uber_last_week."LW Budget PCOS Spend Std",
            uber_last_week."LW Customer-Invoiced Variable Trade % vs Budget (Split)",
            uber_last_week."LW Customer-Invoiced Variable Trade % vs Forecast (Split)",
            uber_last_week."LW Customer-Invoiced Variable Trade % vs LY (Split)",
            uber_last_week."LW Customer-Invoiced Variable Trade Var to Budget (Split)",
            uber_last_week."LW Customer-Invoiced Variable Trade Var to Forecast (Split)",
            uber_last_week."LW Customer-Invoiced Variable Trade Var to LY (Split)",
            uber_last_week."LW CY NRR £ (Combo)",
            uber_last_week."LW Cy Variable Trade Spend",
            uber_last_week."LW Forecast 1 Permanent Discounts £ (Combo)",
            uber_last_week."LW Forecast Added Value Pack (Combo)",
            uber_last_week."LW Forecast Fixed Trade £ (Combo)",
            uber_last_week."LW Forecast Fixed Trade Customer-Invoiced",
            uber_last_week."LW Forecast Fixed Trade Spend",
            uber_last_week."LW Forecast GCAT £ (Combo) Act",
            uber_last_week."LW Forecast GCAT £ (Combo) Std",
            uber_last_week."LW Forecast Gross Amount (Combo)",
            uber_last_week."LW Forecast Net Sales £ (Combo)",
            uber_last_week."LW Forecast Total Customer-Invoiced Trade £ (Combo)",
            uber_last_week."LW Forecast Total Customer-Invoiced Trade Spend (Combo)",
            uber_last_week."LW Forecast Variable Trade £ (Combo) (1)",
            uber_last_week."LW Forecast Variable Trade Spend",
            uber_last_week."Ly Net Sales (Combo)",
            uber_last_week."LW LY NRR £ (Combo)",
            uber_last_week."LW LY Total Customer-Invoiced Trade (Combo)",
            uber_last_week."LW Abs Var Forecast Cust-Inv Trade",
            uber_last_week."LW Abs Var Forecast GCAT Std",
            uber_last_week."LW Abs Var Forecast NRR",
            uber_last_week."LW Abs Var Forecast GCAT Act",
            uber_last_week."LW CY GCAT £ (Combo) Act",
            uber_last_week."LW CY GCAT £ (Combo) Std",
            uber_last_week."LW Forecast NRR £ (Combo)",

            uber_current_week."CW (Budget) Forecast Added Value Pack (Combo)",
            uber_current_week."CW (Budget) Forecast Gross Amount (Combo)",
            uber_current_week."CW (Budget) Forecast Gross Sales £ (Combo)",
            uber_current_week."CW CY Added Value Pack £",
            uber_current_week."CW CY Total Customer-Invoiced Trade Spend (Combo)",
            uber_current_week."CW (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
            uber_current_week."CW Cy PCOS Spend (Combo) Std",
            uber_current_week."CW Cy PCOS Spend (Combo) Act",
            uber_current_week."CW (Budget) Forecast Net Sales £ (Combo)",
            uber_current_week."CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_current_week."CW (Budget) Forecast NRR £ (Combo)",
            uber_current_week."CW (Budget) Forecast Permanent Discounts £ (Combo)",
            uber_current_week."CW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
            uber_current_week."CW (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
            uber_current_week."CW Budget Variable Trade £ (Combo)",
            uber_current_week."CW CY Customer-Invoiced Variable Trade £ (Combo Split)",
            uber_current_week."CW CY Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_current_week."CW CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
            uber_current_week."CW CY Total Customer-Invoiced Trade (Combo)",
            uber_current_week."CW CY Total Variable Trade £ (Combo)",
            uber_current_week."CW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            uber_current_week."CW Forecast PCOS £ (Combo) Act",
            uber_current_week."CW Forecast PCOS £ (Combo) Std",
            uber_current_week."CW Forecast Permanent Discounts £ (Combo)",
            uber_current_week."CW (Budget) Forecast Gross Margin £ Act",
            uber_current_week."CW (Budget) Forecast Gross Margin £ Std",
            uber_current_week."CW (Budget) Forecast Total PCOS Amt £ Std PCOS",
            uber_current_week."CW Budget Fixed Trade Spend",
            uber_current_week."CW Budget PCOS Spend Act",
            uber_current_week."CW Budget PCOS Spend Std",
            uber_current_week."CW Customer-Invoiced Variable Trade % vs Budget (Split)",
            uber_current_week."CW Customer-Invoiced Variable Trade % vs Forecast (Split)",
            uber_current_week."CW Customer-Invoiced Variable Trade % vs LY (Split)",
            uber_current_week."CW Customer-Invoiced Variable Trade Var to Budget (Split)",
            uber_current_week."CW Customer-Invoiced Variable Trade Var to Forecast (Split)",
            uber_current_week."CW Customer-Invoiced Variable Trade Var to LY (Split)",
            uber_current_week."CW CY NRR £ (Combo)",
            uber_current_week."CW Cy Variable Trade Spend",
            uber_current_week."CW Forecast 1 Permanent Discounts £ (Combo)",
            uber_current_week."CW Forecast Added Value Pack (Combo)",
            uber_current_week."CW Forecast Fixed Trade £ (Combo)",
            uber_current_week."CW Forecast Fixed Trade Customer-Invoiced",
            uber_current_week."CW Forecast Fixed Trade Spend",
            uber_current_week."CW Forecast GCAT £ (Combo) Act",
            uber_current_week."CW Forecast GCAT £ (Combo) Std",
            uber_current_week."CW Forecast Gross Amount (Combo)",
            uber_current_week."CW Forecast Net Sales £ (Combo)",
            uber_current_week."CW Forecast Total Customer-Invoiced Trade £ (Combo)",
            uber_current_week."CW Forecast Total Customer-Invoiced Trade Spend (Combo)",
            uber_current_week."CW Forecast Variable Trade £ (Combo) (1)",
            uber_current_week."CW Forecast Variable Trade Spend",
            uber_current_week."CW LY NRR £ (Combo)",
            uber_current_week."CW LY Total Customer-Invoiced Trade (Combo)",
            uber_current_week."CW Abs Var Forecast Cust-Inv Trade",
            uber_current_week."CW Abs Var Forecast GCAT Std",
            uber_current_week."CW Abs Var Forecast NRR",
            uber_current_week."CW Abs Var Forecast GCAT Act",
            uber_current_week."CW CY GCAT £ (Combo) Act",
            uber_current_week."CW CY GCAT £ (Combo) Std",
            uber_current_week."CW Forecast NRR £ (Combo)",
            uber_current_week."CW Forecast Non-Customer Invoiced Fixed Trade £ (LIVE)",
            uber_current_week."CW Forecast PCOS £ (LIVE) Act",
            uber_current_week."CW Forecast PCOS £ (LIVE) Std",
            - (main."CY Total Std PCOS Amt £" + main."CY Total Var to Std PCOS Amt £")
            + main."Forecast PCOS (Std) £"
            + main."Forecast PCOS (Var) £" as "Cy PCOS Spend (Combo) Act",
            - main."CY Total Std PCOS Amt £"
            + main."Forecast PCOS (Std) £" as "Cy PCOS Spend (Combo) Std",
            main."CY Customer-Invoiced Fixed Trade £" + (
                (main."Forecast Customer-Invoiced Fixed Trade £") * -1
            ) as "(Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
            main."CY Customer-Invoiced Fixed Trade £" + (
                (ifnull(main."Forecast Customer-Invoiced Fixed Trade £", 0)) * -1
            ) as "(Budget) Forecast Fixed Trade Customer-Invoiced (Split)",

            (
                (
                    ifnull(
                        main."Cy Invoice Gross Amount £"
                        + main."Forecast Gross Amount £",
                        0
                    )
                ) + (
                    ifnull(
                        (main."Cy AVP Discount £" * -1)
                        + main."Forecast Added Value Pack £",
                        0
                    )
                )
            ) + (
                ifnull(
                    main."CY Permanent Discounts £"
                    + (-1 * main."Forecast Permanent Discounts £"),
                    0
                )
            ) as "(Budget) Forecast Net Sales £ (Combo)",
            ifnull(
                main."CY Permanent Discounts £"
                + (-1 * main."Forecast Permanent Discounts £"),
                0
            ) as "(Budget) Forecast Permanent Discounts £ (Combo)",

            ifnull(
                main."CY Total Customer-Invoiced Trade £"
                + (-1 * main."Forecast Total Customer-Invoiced Trade £"),
                0
            ) as "(Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
            ifnull(
                main."CY Total Customer-Invoiced Trade £"
                + (-1 * main."Forecast Total Customer-Invoiced Trade £"),
                0
            )
            * -1 as "(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
            ifnull(
                main."CY Total Non-Customer Invoiced Fixed Trade £"
                + (main."Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            ) as "(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £",

            (
                ifnull(main."CY Total Std PCOS Amt £", 0)
                + ifnull(main."CY Total Var to Std PCOS Amt £", 0)
                + (
                    ifnull(main."Forecast PCOS (Std) £", 0)
                    + ifnull(main."Forecast PCOS (Var) £", 0)
                )
            ) + (
                -1 * (
                    ifnull((main."Forecast Total Standard PCOS Amt £"), 0)
                    + ifnull((main."Forecast Total Var PCOS Amt £"), 0)
                )
            ) as "(Budget) Forecast Total PCOS Amt £ Act PCOS",

            -1 * ifnull(
                (
                    main."CY Total Variable Trade £"
                    + - main."Forecast Total Variable Trade £"
                ),
                0
            ) as "CY Customer-Invoiced Variable Trade £ (Combo Split)",
            ifnull(
                (
                    (
                        ifnull(
                            main."CY Gross Sales £" + main."CY Permanent Discounts £", 0
                        )
                        + ifnull(main."CY Total Customer-Invoiced Trade £", 0)
                    )
                    + "CY Total PCOS Amt £"
                ),
                0
            )
            + main."CY Total Non-Customer Invoiced Fixed Trade £" as "CY GCAT £ Act",
            ifnull(
                (
                    (
                        ifnull(
                            main."CY Gross Sales £" + main."CY Permanent Discounts £", 0
                        )
                        + ifnull(main."CY Total Customer-Invoiced Trade £", 0)
                    )
                    + main."CY Total Std PCOS Amt £"
                ),
                0
            )
            + main."CY Total Non-Customer Invoiced Fixed Trade £" as "CY GCAT £ Std",
            main."CY Gross Sales £" + "CY Permanent Discounts £" as "CY Net Sales £",
            -1 * (
                main."CY Total Non-Customer Invoiced Fixed Trade £"
                + - (main."Forecast Total Non-Customer Invoiced Fixed Trade £")
            ) as "CY Non-Customer Invoiced Fixed Trade Spend (Combo)",

            - ifnull(
                main."CY Total Non-Customer Invoiced Fixed Trade £"
                + -1 * main."Forecast Total Non-Customer Invoiced Fixed Trade £",
                0
            ) as "Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            -1 * main."CY Total PCOS Amt £"
            + main."Forecast PCOS (Std) £"
            + main."Forecast PCOS (Var) £" as "Forecast PCOS £ (Combo) Act",
            -1 * main."CY Total Std PCOS Amt £"
            + main."Forecast PCOS (Std) £" as "Forecast PCOS £ (Combo) Std",
            main."CY Permanent Discounts £"
            - main."Forecast Permanent Discounts £"
            as "Forecast Permanent Discounts £ (Combo)",
            ifnull(
                main."CY Added Value Pack £" + main."Forecast Added Value Pack £", 0
            ) as "Forecast 1 AVP £ (Combo)",
            (ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)) + (
                ifnull("CY Added Value Pack £" + "Forecast Added Value Pack £", 0)
            ) as "Forecast 1 Gross Sales £ (Combo)",
            ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)
            + (ifnull("CY Added Value Pack £" + "Forecast Added Value Pack £", 0))
            + ifnull(
                "CY Permanent Discounts £" + -1 * "Forecast Permanent Discounts £", 0
            ) as "Forecast 1 Net Sales £ (Combo) ",
            - ifnull(
                "CY Customer-Invoiced Fixed Trade £"
                + - ifnull("Forecast Customer-Invoiced Fixed Trade £", 0),
                0
            ) as "Forecast Fixed Trade Customer-Invoiced (Split)",
            ifnull(
                "Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0
            ) as "Forecast Gross Amount (Combo)",

            case
                when
                    (
                        (
                            ifnull(
                                "Budget (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                    )
                    = 0
                then 0
                else
                    ifnull(
                        "Budget CY GCAT £ (Combo) Act" - (
                            ifnull(
                                "Budget (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                        / (
                            ifnull(
                                "Budget (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        ),
                        0
                    )
            end as "Budget GCAT % vs Budget Act",
            case
                when
                    (
                        (
                            ifnull(
                                "LW (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                    )
                    = 0
                then 0
                else
                    ifnull(
                        "LW CY GCAT £ (Combo) Act" - (
                            ifnull(
                                "LW (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                        / (
                            ifnull(
                                "LW (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        ),
                        0
                    )
            end as "LW GCAT % vs Budget Act",
            case
                when
                    (
                        (
                            ifnull(
                                "CW (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                    )
                    = 0
                then 0
                else
                    ifnull(
                        "CW CY GCAT £ (Combo) Act" - (
                            ifnull(
                                "CW (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                        / (
                            ifnull(
                                "CW (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        ),
                        0
                    )
            end as "CW GCAT % vs Budget Act",
            case
                when
                    (
                        (
                            ifnull(
                                "LE (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                    )
                    = 0
                then 0
                else
                    ifnull(
                        "LE CY GCAT £ (Combo) Act" - (
                            ifnull(
                                "LE (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                        / (
                            ifnull(
                                "LE (Budget) Forecast Gross Margin £ Act",
                                0
                            ) - ifnull(
                                "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        ),
                        0
                    )
            end as "LE GCAT % vs Budget Act",
            case
                when
                    (
                        ifnull(
                            "Budget (Budget) Forecast Gross Margin £ Std",
                            0
                        ) - ifnull(
                            "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                            0
                        )
                    )
                    = 0
                then 0
                else
                    ifnull(
                        "Budget CY GCAT £ (Combo) Std" - (
                            ifnull(
                                "Budget (Budget) Forecast Gross Margin £ Std",
                                0
                            ) - ifnull(
                                "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                        / (
                            ifnull(
                                "Budget (Budget) Forecast Gross Margin £ Std",
                                0
                            ) - ifnull(
                                "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        ),
                        0
                    )
            end as "Budget GCAT % vs Budget Std",
            case
                when
                    (
                        ifnull("LE (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                            "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                            0
                        )
                    )
                    = 0
                then 0
                else
                    ifnull(
                        "LE CY GCAT £ (Combo) Std" - (
                            ifnull(
                                "LE (Budget) Forecast Gross Margin £ Std",
                                0
                            ) - ifnull(
                                "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                        / (
                            ifnull(
                                "LE (Budget) Forecast Gross Margin £ Std",
                                0
                            ) - ifnull(
                                "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        ),
                        0
                    )
            end as "LE GCAT % vs Budget Std",
            case
                when
                    (
                        ifnull("CW (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                            "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                            0
                        )
                    )
                    = 0
                then 0
                else
                    ifnull(
                        "CW CY GCAT £ (Combo) Std" - (
                            ifnull(
                                "CW (Budget) Forecast Gross Margin £ Std",
                                0
                            ) - ifnull(
                                "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                        / (
                            ifnull(
                                "CW (Budget) Forecast Gross Margin £ Std",
                                0
                            ) - ifnull(
                                "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        ),
                        0
                    )
            end as "CW GCAT % vs Budget Std",
            case
                when
                    (
                        ifnull("LW (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                            "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                            0
                        )
                    )
                    = 0
                then 0
                else
                    ifnull(
                        "LW CY GCAT £ (Combo) Std" - (
                            ifnull(
                                "LW (Budget) Forecast Gross Margin £ Std",
                                0
                            ) - ifnull(
                                "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        )
                        / (
                            ifnull(
                                "LW (Budget) Forecast Gross Margin £ Std",
                                0
                            ) - ifnull(
                                "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                                0
                            )
                        ),
                        0
                    )
            end as "LW GCAT % vs Budget Std",
            case
                when "Budget Forecast GCAT £ (Combo) Act" = 0
                then 0
                else
                    ifnull(
                        (
                            "Budget CY GCAT £ (Combo) Act"
                            - "Budget Forecast GCAT £ (Combo) Act"
                        )
                        / "Budget Forecast GCAT £ (Combo) Act",
                        0
                    )
            end as "Budget GCAT % vs Forecast Act",
            case
                when "LW Forecast GCAT £ (Combo) Act" = 0
                then 0
                else
                    ifnull(
                        ("LW CY GCAT £ (Combo) Act" - "LW Forecast GCAT £ (Combo) Act")
                        / "LW Forecast GCAT £ (Combo) Act",
                        0
                    )
            end as "LW GCAT % vs Forecast Act",
            case
                when "CW Forecast GCAT £ (Combo) Act" = 0
                then 0
                else
                    ifnull(
                        ("CW CY GCAT £ (Combo) Act" - "CW Forecast GCAT £ (Combo) Act")
                        / "CW Forecast GCAT £ (Combo) Act",
                        0
                    )
            end as "CW GCAT % vs Forecast Act",
            case
                when "LE Forecast GCAT £ (Combo) Act" = 0
                then 0
                else
                    ifnull(
                        ("LE CY GCAT £ (Combo) Act" - "LE Forecast GCAT £ (Combo) Act")
                        / "LE Forecast GCAT £ (Combo) Act",
                        0
                    )
            end as "LE GCAT % vs Forecast Act",
            case
                when "Budget Forecast GCAT £ (Combo) Std" = 0
                then 0
                else
                    ifnull(
                        (
                            "Budget CY GCAT £ (Combo) Std"
                            - "Budget Forecast GCAT £ (Combo) Std"
                        )
                        / "Budget Forecast GCAT £ (Combo) Std",
                        0
                    )
            end as "Budget GCAT % vs Forecast Std",
            case
                when "LW Forecast GCAT £ (Combo) Std" = 0
                then 0
                else
                    ifnull(
                        ("LW CY GCAT £ (Combo) Std" - "LW Forecast GCAT £ (Combo) Std")
                        / "LW Forecast GCAT £ (Combo) Std",
                        0
                    )
            end as "LW GCAT % vs Forecast Std",
            case
                when "CW Forecast GCAT £ (Combo) Std" = 0
                then 0
                else
                    ifnull(
                        ("CW CY GCAT £ (Combo) Std" - "CW Forecast GCAT £ (Combo) Std")
                        / "CW Forecast GCAT £ (Combo) Std",
                        0
                    )
            end as "CW GCAT % vs Forecast Std",
            case
                when "LE Forecast GCAT £ (Combo) Std" = 0
                then 0
                else
                    ifnull(
                        ("LE CY GCAT £ (Combo) Std" - "LE Forecast GCAT £ (Combo) Std")
                        / "LE Forecast GCAT £ (Combo) Std",
                        0
                    )
            end as "LE GCAT % vs Forecast Std",
            ifnull("LY Gross Sales £", 0)
            + ifnull("LY Permanent Discounts £", 0) as "LY Net Sales £",
            ifnull("LY Net Sales £", 0)
            + ifnull("LY Total Customer-Invoiced Trade £", 0) as "LY NRR £",
            ifnull(
                "LY NRR £"
                + ifnull("LY Total Std PCOS Amt £", 0)
                + ifnull("LY Total Var to Std PCOS Amt £", 0),
                0
            ) + ifnull(
                "LY Total Non-Customer Invoiced Fixed Trade £", 0
            ) as "LY GCAT £ Act",
            ifnull("LY NRR £" + "LY Total Std PCOS Amt £", 0) + ifnull(
                "LY Total Non-Customer Invoiced Fixed Trade £", 0
            ) as "LY GCAT £ Std",
            ifnull("Ly Promo Fixed Funding £", 0)
            + ifnull("Ly Fixed Annual Pymts £", 0)
            + ifnull("Ly Direct Shopper Marking £", 0)
            + ifnull("Ly Other Direct Pymts £", 0)
            + "LY Total Non-Customer Invoiced Fixed Trade £"
            as "LY Total Fixed Trade £",
            ifnull("LY Total Std PCOS Amt £", 0)
            + ifnull("LY Total Var to Std PCOS Amt £", 0) as "LY Total PCOS Amt £",
            case
                when "LY GCAT £ Std" = 0
                then 0
                else
                    ("Budget CY GCAT £ (Combo) Std" - ("LY GCAT £ Std"))
                    / ("LY GCAT £ Std")
            end as "Budget GCAT % YoY Std",
            case
                when "LY GCAT £ Std" = 0
                then 0
                else
                    ("LW CY GCAT £ (Combo) Std" - ("LY GCAT £ Std")) / ("LY GCAT £ Std")
            end as "LW GCAT % YoY Std",
            case
                when "LY GCAT £ Std" = 0
                then 0
                else
                    ("LE CY GCAT £ (Combo) Std" - ("LY GCAT £ Std")) / ("LY GCAT £ Std")
            end as "LE GCAT % YoY Std",
            case
                when "LY GCAT £ Std" = 0
                then 0
                else
                    ("CW CY GCAT £ (Combo) Std" - ("LY GCAT £ Std")) / ("LY GCAT £ Std")
            end as "CW GCAT % YoY Std",
            case
                when "LY GCAT £ Act" = 0
                then 0
                else
                    ifnull(
                        ("Budget CY GCAT £ (Combo) Act" - ("LY GCAT £ Act"))
                        / ("LY GCAT £ Act"),
                        0
                    )
            end as "Budget GCAT % YoY Act",
            case
                when "LY GCAT £ Act" = 0
                then 0
                else
                    ifnull(
                        ("LE CY GCAT £ (Combo) Act" - ("LY GCAT £ Act"))
                        / ("LY GCAT £ Act"),
                        0
                    )
            end as "LE GCAT % YoY Act",
            case
                when "LY GCAT £ Act" = 0
                then 0
                else
                    ifnull(
                        ("CW CY GCAT £ (Combo) Act" - ("LY GCAT £ Act"))
                        / ("LY GCAT £ Act"),
                        0
                    )
            end as "CW GCAT % YoY Act",
            case
                when "LY GCAT £ Act" = 0
                then 0
                else
                    ifnull(
                        ("LW CY GCAT £ (Combo) Act" - ("LY GCAT £ Act"))
                        / ("LY GCAT £ Act"),
                        0
                    )
            end as "LW GCAT % YoY Act",
            ifnull("Budget CY GCAT £ (Combo) Std", 0) - (
                ifnull(
                    ifnull("Budget (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                        "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                        0
                    ),
                    0
                )
            ) as "Budget GCAT Var to Budget Std",
            ifnull("LE CY GCAT £ (Combo) Std", 0) - (
                ifnull(
                    ifnull("LE (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                        "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                        0
                    ),
                    0
                )
            ) as "LE GCAT Var to Budget Std",
            ifnull("CW CY GCAT £ (Combo) Std", 0) - (
                ifnull(
                    ifnull("CW (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                        "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                        0
                    ),
                    0
                )
            ) as "CW GCAT Var to Budget Std",
            ifnull("LW CY GCAT £ (Combo) Std", 0) - (
                ifnull(
                    ifnull("LW (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                        "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                        0
                    ),
                    0
                )
            ) as "LW GCAT Var to Budget Std",
            ifnull("Budget CY GCAT £ (Combo) Act", 0) - (
                ifnull(
                    ifnull("Budget (Budget) Forecast Gross Margin £ Act", 0) - ifnull(
                        "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                        0
                    ),
                    0
                )
            ) as "Budget GCAT Var to Budget Act",
            ifnull("LW CY GCAT £ (Combo) Act", 0) - (
                ifnull(
                    ifnull("LW (Budget) Forecast Gross Margin £ Act", 0) - ifnull(
                        "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                        0
                    ),
                    0
                )
            ) as "LW GCAT Var to Budget Act",
            ifnull("CW CY GCAT £ (Combo) Act", 0) - (
                ifnull(
                    ifnull("CW (Budget) Forecast Gross Margin £ Act", 0) - ifnull(
                        "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                        0
                    ),
                    0
                )
            ) as "CW GCAT Var to Budget Act",
            ifnull("LE CY GCAT £ (Combo) Act", 0) - (
                ifnull(
                    ifnull("LE (Budget) Forecast Gross Margin £ Act", 0) - ifnull(
                        "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                        0
                    ),
                    0
                )
            ) as "LE GCAT Var to Budget Act",
            ifnull("Budget CY GCAT £ (Combo) Std", 0) - ifnull(
                "Budget Forecast GCAT £ (Combo) Std", 0
            ) as "Budget GCAT Var to Forecast Std",
            ifnull("CW CY GCAT £ (Combo) Std", 0) - ifnull(
                "CW Forecast GCAT £ (Combo) Std", 0
            ) as "CW GCAT Var to Forecast Std",
            ifnull("LW CY GCAT £ (Combo) Std", 0) - ifnull(
                "LW Forecast GCAT £ (Combo) Std", 0
            ) as "LW GCAT Var to Forecast Std",
            ifnull("LE CY GCAT £ (Combo) Std", 0) - ifnull(
                "LE Forecast GCAT £ (Combo) Std", 0
            ) as "LE GCAT Var to Forecast Std",
            ifnull("Budget CY GCAT £ (Combo) Act", 0) - ifnull(
                "Budget Forecast GCAT £ (Combo) Act", 0
            ) as "Budget GCAT Var to Forecast Act",
            ifnull("LE CY GCAT £ (Combo) Act", 0) - ifnull(
                "LE Forecast GCAT £ (Combo) Act", 0
            ) as "LE GCAT Var to Forecast Act",
            ifnull("LW CY GCAT £ (Combo) Act", 0) - ifnull(
                "LW Forecast GCAT £ (Combo) Act", 0
            ) as "LW GCAT Var to Forecast Act",
            ifnull("CW CY GCAT £ (Combo) Act", 0) - ifnull(
                "CW Forecast GCAT £ (Combo) Act", 0
            ) as "CW GCAT Var to Forecast Act",
            ifnull("Budget CY GCAT £ (Combo) Std", 0)
            - (ifnull("LY GCAT £ Std", 0)) as "Budget GCAT Var to LY Std",
            ifnull("Budget CY GCAT £ (Combo) Act", 0)
            - (ifnull("LY GCAT £ Act", 0)) as "Budget GCAT Var to LY Act",

            ifnull("LE CY GCAT £ (Combo) Std", 0)
            - (ifnull("LY GCAT £ Std", 0)) as "LE GCAT Var to LY Std",
            ifnull("LE CY GCAT £ (Combo) Act", 0)
            - (ifnull("LY GCAT £ Act", 0)) as "LE GCAT Var to LY Act",

            ifnull("LW CY GCAT £ (Combo) Std", 0)
            - (ifnull("LY GCAT £ Std", 0)) as "LW GCAT Var to LY Std",
            ifnull("LW CY GCAT £ (Combo) Act", 0)
            - (ifnull("LY GCAT £ Act", 0)) as "LW GCAT Var to LY Act",

            ifnull("CW CY GCAT £ (Combo) Std", 0)
            - (ifnull("LY GCAT £ Std", 0)) as "CW GCAT Var to LY Std",
            ifnull("CW CY GCAT £ (Combo) Act", 0)
            - (ifnull("LY GCAT £ Act", 0)) as "CW GCAT Var to LY Act",
            "LY Total Variable Trade £"
            * -1 as "LY Customer-Invoiced Variable Trade £ (Combo Split)",
            ifnull("LY Total Fixed Trade £", 0) * -1 as "Ly Fixed Trade Spend",
            - ifnull(
                "LY Total Non-Customer Invoiced Fixed Trade £", 0
            ) as "LY Non-Customer Invoiced Fixed Trade Spend",
            "LY Total Std PCOS Amt £" * -1 as "Ly PCOS Spend Std",
            "LY Total PCOS Amt £" * -1 as "Ly PCOS Spend Act",
            - "LY Total Customer-Invoiced Trade £"
            as "LY Total Customer-Invoiced Trade Spend",
            ifnull("LY Total Variable Trade £", 0) * -1 as "Ly Variable Trade Spend",
            case
                when "Budget (Budget) Forecast Net Sales £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            "Forecast 1 Net Sales £ (Combo) "
                            - ("Budget (Budget) Forecast Net Sales £ (Combo)")
                        )
                        / ("Budget (Budget) Forecast Net Sales £ (Combo)"),
                        0
                    )
            end as "Budget Net Sales % vs Budget",
            case
                when "LW (Budget) Forecast Net Sales £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            "Forecast 1 Net Sales £ (Combo) "
                            - ("LW (Budget) Forecast Net Sales £ (Combo)")
                        )
                        / ("LW (Budget) Forecast Net Sales £ (Combo)"),
                        0
                    )
            end as "LW Net Sales % vs Budget",
            case
                when "CW (Budget) Forecast Net Sales £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            "Forecast 1 Net Sales £ (Combo) "
                            - ("CW (Budget) Forecast Net Sales £ (Combo)")
                        )
                        / ("CW (Budget) Forecast Net Sales £ (Combo)"),
                        0
                    )
            end as "CW Net Sales % vs Budget",
            case
                when "LE (Budget) Forecast Net Sales £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            "Forecast 1 Net Sales £ (Combo) "
                            - ("LE (Budget) Forecast Net Sales £ (Combo)")
                        )
                        / ("LE (Budget) Forecast Net Sales £ (Combo)"),
                        0
                    )
            end as "LE Net Sales % vs Budget",
            case
                when "Budget Forecast Net Sales £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            "Forecast 1 Net Sales £ (Combo) "
                            - "Budget Forecast Net Sales £ (Combo)"
                        )
                        / "Budget Forecast Net Sales £ (Combo)",
                        0
                    )
            end as "Budget Net Sales % vs Forecast",
            case
                when "LE Forecast Net Sales £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            "Forecast 1 Net Sales £ (Combo) "
                            - "LE Forecast Net Sales £ (Combo)"
                        )
                        / "LE Forecast Net Sales £ (Combo)",
                        0
                    )
            end as "LE Net Sales % vs Forecast",
            case
                when "CW Forecast Net Sales £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            "Forecast 1 Net Sales £ (Combo) "
                            - "CW Forecast Net Sales £ (Combo)"
                        )
                        / "CW Forecast Net Sales £ (Combo)",
                        0
                    )
            end as "CW Net Sales % vs Forecast",
            case
                when "LW Forecast Net Sales £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            "Forecast 1 Net Sales £ (Combo) "
                            - "LW Forecast Net Sales £ (Combo)"
                        )
                        / "LW Forecast Net Sales £ (Combo)",
                        0
                    )
            end as "LW Net Sales % vs Forecast",
            case
                when "LY Net Sales £" = 0
                then 0
                else
                    ifnull(
                        ("Forecast 1 Net Sales £ (Combo) " - ("LY Net Sales £"))
                        / ("LY Net Sales £"),
                        0
                    )
            end as "Net Sales % YoY",
            ifnull(
                (
                    "Forecast 1 Net Sales £ (Combo) "
                    - ("Budget (Budget) Forecast Net Sales £ (Combo)")
                ),
                0
            ) as "Budget Net Sales Var to Budget",
            ifnull(
                (
                    "Forecast 1 Net Sales £ (Combo) "
                    - ("LW (Budget) Forecast Net Sales £ (Combo)")
                ),
                0
            ) as "LW Net Sales Var to Budget",
            ifnull(
                (
                    "Forecast 1 Net Sales £ (Combo) "
                    - ("CW (Budget) Forecast Net Sales £ (Combo)")
                ),
                0
            ) as "CW Net Sales Var to Budget",
            ifnull(
                (
                    "Forecast 1 Net Sales £ (Combo) "
                    - ("LE (Budget) Forecast Net Sales £ (Combo)")
                ),
                0
            ) as "LE Net Sales Var to Budget",
            ifnull(
                (
                    "Forecast 1 Net Sales £ (Combo) "
                    - "Budget Forecast Net Sales £ (Combo)"
                ),
                0
            ) as "Budget Net Sales Var to Forecast",
            ifnull(
                ("Forecast 1 Net Sales £ (Combo) " - "CW Forecast Net Sales £ (Combo)"),
                0
            ) as "CW Net Sales Var to Forecast",
            ifnull(
                ("Forecast 1 Net Sales £ (Combo) " - "LW Forecast Net Sales £ (Combo)"),
                0
            ) as "LW Net Sales Var to Forecast",
            ifnull(
                ("Forecast 1 Net Sales £ (Combo) " - "LE Forecast Net Sales £ (Combo)"),
                0
            ) as "LE Net Sales Var to Forecast",
            ifnull(
                (
                    "Forecast 1 Net Sales £ (Combo) "
                    - (uber_last_week."Ly Net Sales (Combo)")
                ),
                0
            ) as "Net Sales Var to LY",
            ifnull(
                ("Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            )
            * -1 as "Budget Non-Customer Fixed Trade Var to Budget",
            ifnull(
                ("LW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            )
            * -1 as "LW Non-Customer Fixed Trade Var to Budget",
            ifnull(
                ("LE CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            )
            * -1 as "LE Non-Customer Fixed Trade Var to Budget",
            ifnull(
                ("CW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            )
            * -1 as "CW Non-Customer Fixed Trade Var to Budget",

            ifnull(
                ("Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            ) as "Budget Non-Customer Inv Trade % vs Budget",
            ifnull(
                ("LW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            ) as "LW Non-Customer Inv Trade % vs Budget",
            ifnull(
                ("LE CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            ) as "LE Non-Customer Inv Trade % vs Budget",
            ifnull(
                ("CW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            ) as "CW Non-Customer Inv Trade % vs Budget",

            case
                when "Budget Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            - ("Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                            - (
                                "Budget Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"
                            )
                        ) / (
                            "Budget Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"
                        ),
                        0
                    )
            end as "Budget Non-Customer Inv Trade % vs Forecast",

            case
                when "LW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            - ("LW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                            - (
                                "LW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"
                            )
                        )
                        / ("LW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"),
                        0
                    )
            end as "LW Non-Customer Inv Trade % vs Forecast",

            case
                when "LE Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            - ("LE CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                            - (
                                "LE Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"
                            )
                        )
                        / ("LE Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"),
                        0
                    )
            end as "LE Non-Customer Inv Trade % vs Forecast",

            case
                when "CW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" = 0
                then 0
                else
                    ifnull(
                        (
                            - ("CW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                            - (
                                "CW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"
                            )
                        )
                        / ("CW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"),
                        0
                    )
            end as "CW Non-Customer Inv Trade % vs Forecast",

            case
                when nvl("LY Total Non-Customer Invoiced Fixed Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                            - ("LY Total Non-Customer Invoiced Fixed Trade £")
                        )
                        / ("LY Total Non-Customer Invoiced Fixed Trade £"),
                        0
                    )
            end as "Budget Non-Customer Inv Trade % YoY",
            case
                when nvl("LY Total Non-Customer Invoiced Fixed Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LE CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                            - ("LY Total Non-Customer Invoiced Fixed Trade £")
                        )
                        / ("LY Total Non-Customer Invoiced Fixed Trade £"),
                        0
                    )
            end as "LE Non-Customer Inv Trade % YoY",
            case
                when nvl("LY Total Non-Customer Invoiced Fixed Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                            - ("LY Total Non-Customer Invoiced Fixed Trade £")
                        )
                        / ("LY Total Non-Customer Invoiced Fixed Trade £"),
                        0
                    )
            end as "LW Non-Customer Inv Trade % YoY",
            case
                when nvl("LY Total Non-Customer Invoiced Fixed Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("CW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                            - ("LY Total Non-Customer Invoiced Fixed Trade £")
                        )
                        / ("LY Total Non-Customer Invoiced Fixed Trade £"),
                        0
                    )
            end as "CW Non-Customer Inv Trade % YoY",

            ifnull(
                - ("Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("Budget Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"),
                0
            ) as "Budget Non-Customer Inv Trade Var to Forecast",
            ifnull(
                - ("LW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("LW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"),
                0
            ) as "LW Non-Customer Inv Trade Var to Forecast",

            ifnull(
                - ("CW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("CW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"),
                0
            ) as "CW Non-Customer Inv Trade Var to Forecast",

            ifnull(
                - ("LE CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("LE Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"),
                0
            ) as "LE Non-Customer Inv Trade Var to Forecast",

            ifnull(
                - ("Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("LY Non-Customer Invoiced Fixed Trade Spend"),
                0
            ) as "Budget Non-Customer Inv Trade Var to LY",
            ifnull(
                - ("LW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("LY Non-Customer Invoiced Fixed Trade Spend"),
                0
            ) as "LW Non-Customer Inv Trade Var to LY",
            ifnull(
                - ("CW CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("LY Non-Customer Invoiced Fixed Trade Spend"),
                0
            ) as "CW Non-Customer Inv Trade Var to LY",
            ifnull(
                - ("LE CY Non-Customer Invoiced Fixed Trade £ (Combo)")
                - ("LY Non-Customer Invoiced Fixed Trade Spend"),
                0
            ) as "LE Non-Customer Inv Trade Var to LY",
            case
                when nvl("Budget (Budget) Forecast NRR £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            "Budget CY NRR £ (Combo)"
                            - ("Budget (Budget) Forecast NRR £ (Combo)")
                        )
                        / ("Budget (Budget) Forecast NRR £ (Combo)"),
                        0
                    )
            end as "Budget NRR % Var to Budget",
            case
                when nvl("CW (Budget) Forecast NRR £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("CW CY NRR £ (Combo)" - ("CW (Budget) Forecast NRR £ (Combo)"))
                        / ("CW (Budget) Forecast NRR £ (Combo)"),
                        0
                    )
            end as "CW NRR % Var to Budget",
            case
                when nvl("LE (Budget) Forecast NRR £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("LE CY NRR £ (Combo)" - ("LE (Budget) Forecast NRR £ (Combo)"))
                        / ("LE (Budget) Forecast NRR £ (Combo)"),
                        0
                    )
            end as "LE NRR % Var to Budget",
            case
                when nvl("LW (Budget) Forecast NRR £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("LW CY NRR £ (Combo)" - ("LW (Budget) Forecast NRR £ (Combo)"))
                        / ("LW (Budget) Forecast NRR £ (Combo)"),
                        0
                    )
            end as "LW NRR % Var to Budget",
            case
                when nvl("Budget Forecast NRR £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("Budget CY NRR £ (Combo)" - "Budget Forecast NRR £ (Combo)")
                        / "Budget Forecast NRR £ (Combo)",
                        0
                    )
            end as "Budget NRR % Var to Forecast",
            case
                when nvl("LW Forecast NRR £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("LW CY NRR £ (Combo)" - "LW Forecast NRR £ (Combo)")
                        / "LW Forecast NRR £ (Combo)",
                        0
                    )
            end as "LW NRR % Var to Forecast",
            case
                when nvl("CW Forecast NRR £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("CW CY NRR £ (Combo)" - "CW Forecast NRR £ (Combo)")
                        / "CW Forecast NRR £ (Combo)",
                        0
                    )
            end as "CW NRR % Var to Forecast",
            case
                when nvl("LE Forecast NRR £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("LE CY NRR £ (Combo)" - "Budget Forecast NRR £ (Combo)")
                        / "LE Forecast NRR £ (Combo)",
                        0
                    )
            end as "LE NRR % Var to Forecast",
            case
                when nvl("LY NRR £", 0) = 0
                then 0
                else
                    ifnull(("Budget CY NRR £ (Combo)" - ("LY NRR £")) / ("LY NRR £"), 0)
            end as "Budget NRR % YoY",
            case
                when nvl("LY NRR £", 0) = 0
                then 0
                else ifnull(("LW CY NRR £ (Combo)" - ("LY NRR £")) / ("LY NRR £"), 0)
            end as "LW NRR % YoY",
            case
                when nvl("LY NRR £", 0) = 0
                then 0
                else ifnull(("CW CY NRR £ (Combo)" - ("LY NRR £")) / ("LY NRR £"), 0)
            end as "CW NRR % YoY",
            case
                when nvl("LY NRR £", 0) = 0
                then 0
                else ifnull(("LE CY NRR £ (Combo)" - ("LY NRR £")) / ("LY NRR £"), 0)
            end as "LE NRR % YoY",
            ifnull("Budget CY NRR £ (Combo)", 0) - (
                ifnull("Budget (Budget) Forecast NRR £ (Combo)", 0)
            ) as "Budget NRR Var to Budget",
            ifnull("CW CY NRR £ (Combo)", 0) - (
                ifnull("CW (Budget) Forecast NRR £ (Combo)", 0)
            ) as "CW NRR Var to Budget",
            ifnull("LE CY NRR £ (Combo)", 0) - (
                ifnull("LE (Budget) Forecast NRR £ (Combo)", 0)
            ) as "LE NRR Var to Budget",
            ifnull("LW CY NRR £ (Combo)", 0) - (
                ifnull("LW (Budget) Forecast NRR £ (Combo)", 0)
            ) as "LW NRR Var to Budget",
            ifnull("Budget CY NRR £ (Combo)", 0) - ifnull(
                "Budget Forecast NRR £ (Combo)", 0
            ) as "Budget NRR Var to Forecast",
            ifnull("LW CY NRR £ (Combo)", 0)
            - ifnull("LW Forecast NRR £ (Combo)", 0) as "LW NRR Var to Forecast",
            ifnull("CW CY NRR £ (Combo)", 0)
            - ifnull("CW Forecast NRR £ (Combo)", 0) as "CW NRR Var to Forecast",
            ifnull("LE CY NRR £ (Combo)", 0)
            - ifnull("LE Forecast NRR £ (Combo)", 0) as "LE NRR Var to Forecast",
            "Budget CY NRR £ (Combo)" - ("LY NRR £") as "Budget NRR YoY",
            "LE CY NRR £ (Combo)" - ("LY NRR £") as "LE NRR YoY",
            "CW CY NRR £ (Combo)" - ("LY NRR £") as "CW NRR YoY",
            "LW CY NRR £ (Combo)" - ("LY NRR £") as "LW NRR YoY",
            case
                when nvl("Budget Budget PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("Budget Cy PCOS Spend (Combo) Std")
                            - ("Budget Budget PCOS Spend Std")
                        )
                        / ("Budget Budget PCOS Spend Std"),
                        0
                    )
            end as "Budget PCOS % vs Budget Std",
            case
                when nvl("LE Budget PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LE Cy PCOS Spend (Combo) Std")
                            - ("LE Budget PCOS Spend Std")
                        )
                        / ("LE Budget PCOS Spend Std"),
                        0
                    )
            end as "LE PCOS % vs Budget Std",
            case
                when nvl("CW Budget PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("CW Cy PCOS Spend (Combo) Std")
                            - ("CW Budget PCOS Spend Std")
                        )
                        / ("CW Budget PCOS Spend Std"),
                        0
                    )
            end as "CW PCOS % vs Budget Std",
            case
                when nvl("LW Budget PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LW Cy PCOS Spend (Combo) Std")
                            - ("LW Budget PCOS Spend Std")
                        )
                        / ("LW Budget PCOS Spend Std"),
                        0
                    )
            end as "LW PCOS % vs Budget Std",

            case
                when nvl("Budget Forecast PCOS £ (Combo) Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("Budget Cy PCOS Spend (Combo) Act")
                            - ("Budget Forecast PCOS £ (Combo) Std")
                        )
                        / ("Budget Forecast PCOS £ (Combo) Std"),
                        0
                    )
            end as "Budget PCOS % vs Budget Act",
            case
                when nvl("LE Forecast PCOS £ (Combo) Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LE Cy PCOS Spend (Combo) Act")
                            - ("LE Forecast PCOS £ (Combo) Std")
                        )
                        / ("LE Forecast PCOS £ (Combo) Std"),
                        0
                    )
            end as "LE PCOS % vs Budget Act",
            case
                when nvl("CW Forecast PCOS £ (Combo) Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("CW Cy PCOS Spend (Combo) Act")
                            - ("CW Forecast PCOS £ (Combo) Std")
                        )
                        / ("CW Forecast PCOS £ (Combo) Std"),
                        0
                    )
            end as "CW PCOS % vs Budget Act",
            case
                when nvl("LW Forecast PCOS £ (Combo) Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LW Cy PCOS Spend (Combo) Act")
                            - ("LW Forecast PCOS £ (Combo) Std")
                        )
                        / ("LW Forecast PCOS £ (Combo) Std"),
                        0
                    )
            end as "LW PCOS % vs Budget Act",

            case
                when nvl("Budget Budget PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("Budget Cy PCOS Spend (Combo) Std")
                            - ("Budget Budget PCOS Spend Std")
                        )
                        / ("Budget Budget PCOS Spend Std"),
                        0
                    )
            end as "Budget PCOS % vs Forecast Std",
            case
                when nvl("LE Budget PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LE Cy PCOS Spend (Combo) Std")
                            - ("LE Budget PCOS Spend Std")
                        )
                        / ("LE Budget PCOS Spend Std"),
                        0
                    )
            end as "LE PCOS % vs Forecast Std",
            case
                when nvl("CW Budget PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("CW Cy PCOS Spend (Combo) Std")
                            - ("CW Budget PCOS Spend Std")
                        )
                        / ("CW Budget PCOS Spend Std"),
                        0
                    )
            end as "CW PCOS % vs Forecast Std",
            case
                when nvl("LW Budget PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LW Cy PCOS Spend (Combo) Std")
                            - ("LW Budget PCOS Spend Std")
                        )
                        / ("LW Budget PCOS Spend Std"),
                        0
                    )
            end as "LW PCOS % vs Forecast Std",
            case
                when nvl("Budget Budget PCOS Spend Act", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("Budget Cy PCOS Spend (Combo) Act")
                            - ("Budget Budget PCOS Spend Act")
                        )
                        / ("Budget Budget PCOS Spend Act"),
                        0
                    )
            end as "Budget PCOS % vs Forecast Act",
            case
                when nvl("LE Budget PCOS Spend Act", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LE Cy PCOS Spend (Combo) Act")
                            - ("LE Budget PCOS Spend Act")
                        )
                        / ("LE Budget PCOS Spend Act"),
                        0
                    )
            end as "LE PCOS % vs Forecast Act",
            case
                when nvl("CW Budget PCOS Spend Act", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("CW Cy PCOS Spend (Combo) Act")
                            - ("CW Budget PCOS Spend Act")
                        )
                        / ("CW Budget PCOS Spend Act"),
                        0
                    )
            end as "CW PCOS % vs Forecast Act",
            case
                when nvl("LW Budget PCOS Spend Act", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LW Cy PCOS Spend (Combo) Act")
                            - ("LW Budget PCOS Spend Act")
                        )
                        / ("LW Budget PCOS Spend Act"),
                        0
                    )
            end as "LW PCOS % vs Forecast Act",
            case
                when nvl("Ly PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (("Budget Cy PCOS Spend (Combo) Std") - ("Ly PCOS Spend Std"))
                        / ("Ly PCOS Spend Std"),
                        0
                    )
            end as "Budget PCOS % YoY Std",
            case
                when nvl("Ly PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (("LW Cy PCOS Spend (Combo) Std") - ("Ly PCOS Spend Std"))
                        / ("Ly PCOS Spend Std"),
                        0
                    )
            end as "LW PCOS % YoY Std",
            case
                when nvl("Ly PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (("CW Cy PCOS Spend (Combo) Std") - ("Ly PCOS Spend Std"))
                        / ("Ly PCOS Spend Std"),
                        0
                    )
            end as "CW PCOS % YoY Std",
            case
                when nvl("Ly PCOS Spend Std", 0) = 0
                then 0
                else
                    ifnull(
                        (("LE Cy PCOS Spend (Combo) Std") - ("Ly PCOS Spend Std"))
                        / ("Ly PCOS Spend Std"),
                        0
                    )
            end as "LE PCOS % YoY Std",

            case
                when nvl("Ly PCOS Spend Act", 0) = 0
                then 0
                else
                    ifnull(
                        (("Budget Cy PCOS Spend (Combo) Act") - ("Ly PCOS Spend Act"))
                        / ("Ly PCOS Spend Act"),
                        0
                    )
            end as "Budget PCOS % YoY Act",
            case
                when nvl("Ly PCOS Spend Act", 0) = 0
                then 0
                else
                    ifnull(
                        (("LW Cy PCOS Spend (Combo) Act") - ("Ly PCOS Spend Act"))
                        / ("Ly PCOS Spend Act"),
                        0
                    )
            end as "LW PCOS % YoY Act",
            case
                when nvl("Ly PCOS Spend Act", 0) = 0
                then 0
                else
                    ifnull(
                        (("CW Cy PCOS Spend (Combo) Act") - ("Ly PCOS Spend Act"))
                        / ("Ly PCOS Spend Act"),
                        0
                    )
            end as "CW PCOS % YoY Act",
            case
                when nvl("Ly PCOS Spend Act", 0) = 0
                then 0
                else
                    ifnull(
                        (("LE Cy PCOS Spend (Combo) Act") - ("Ly PCOS Spend Act"))
                        / ("Ly PCOS Spend Act"),
                        0
                    )
            end as "LE PCOS % YoY Act",
            ifnull(
                ("Budget Cy PCOS Spend (Combo) Std") - ("Budget Budget PCOS Spend Std"),
                0
            ) as "Budget PCOS Var to Budget Std",
            ifnull(
                ("LE Cy PCOS Spend (Combo) Std") - ("LE Budget PCOS Spend Std"), 0
            ) as "LE PCOS Var to Budget Std",
            ifnull(
                ("LW Cy PCOS Spend (Combo) Std") - ("LW Budget PCOS Spend Std"), 0
            ) as "LW PCOS Var to Budget Std",
            ifnull(
                ("CW Cy PCOS Spend (Combo) Std") - ("CW Budget PCOS Spend Std"), 0
            ) as "CW PCOS Var to Budget Std",

            ifnull(
                ("Budget Cy PCOS Spend (Combo) Act") - ("Budget Budget PCOS Spend Act"),
                0
            ) as "Budget PCOS Var to Budget Act",
            ifnull(
                ("LE Cy PCOS Spend (Combo) Act") - ("LE Budget PCOS Spend Act"), 0
            ) as "LE PCOS Var to Budget Act",
            ifnull(
                ("LW Cy PCOS Spend (Combo) Act") - ("LW Budget PCOS Spend Act"), 0
            ) as "LW PCOS Var to Budget Act",
            ifnull(
                ("CW Cy PCOS Spend (Combo) Act") - ("CW Budget PCOS Spend Act"), 0
            ) as "CW PCOS Var to Budget Act",

            ifnull(
                ("Budget Cy PCOS Spend (Combo) Std")
                - ("Budget Forecast PCOS £ (Combo) Std"),
                0
            ) as "Budget PCOS Var to Forecast Std",
            ifnull(
                ("LE Cy PCOS Spend (Combo) Std") - ("LE Forecast PCOS £ (Combo) Std"), 0
            ) as "LE PCOS Var to Forecast Std",
            ifnull(
                ("LW Cy PCOS Spend (Combo) Std") - ("LW Forecast PCOS £ (Combo) Std"), 0
            ) as "LW PCOS Var to Forecast Std",
            ifnull(
                ("CW Cy PCOS Spend (Combo) Std") - ("CW Forecast PCOS £ (Combo) Std"), 0
            ) as "CW PCOS Var to Forecast Std",

            ifnull(
                ("Budget Cy PCOS Spend (Combo) Act")
                - ("Budget Forecast PCOS £ (Combo) Act"),
                0
            ) as "Budget PCOS Var to Forecast Act",
            ifnull(
                ("LE Cy PCOS Spend (Combo) Act") - ("LE Forecast PCOS £ (Combo) Act"), 0
            ) as "LE PCOS Var to Forecast Act",
            ifnull(
                ("LW Cy PCOS Spend (Combo) Act") - ("LW Forecast PCOS £ (Combo) Act"), 0
            ) as "LW PCOS Var to Forecast Act",
            ifnull(
                ("CW Cy PCOS Spend (Combo) Act") - ("CW Forecast PCOS £ (Combo) Act"), 0
            ) as "CW PCOS Var to Forecast Act",

            ifnull(
                ("Budget Cy PCOS Spend (Combo) Std") - ("Ly PCOS Spend Std"), 0
            ) as "Budget PCOS Var to LY Std",
            ifnull(
                ("LE Cy PCOS Spend (Combo) Std") - ("Ly PCOS Spend Std"), 0
            ) as "LE PCOS Var to LY Std",
            ifnull(
                ("LW Cy PCOS Spend (Combo) Std") - ("Ly PCOS Spend Std"), 0
            ) as "LW PCOS Var to LY Std",
            ifnull(
                ("CW Cy PCOS Spend (Combo) Std") - ("Ly PCOS Spend Std"), 0
            ) as "CW PCOS Var to LY Std",

            ifnull(
                ("Budget Cy PCOS Spend (Combo) Act") - ("Ly PCOS Spend Act"), 0
            ) as "Budget PCOS Var to LY Act",
            ifnull(
                ("LE Cy PCOS Spend (Combo) Act") - ("Ly PCOS Spend Act"), 0
            ) as "LE PCOS Var to LY Act",
            ifnull(
                ("LW Cy PCOS Spend (Combo) Act") - ("Ly PCOS Spend Act"), 0
            ) as "LW PCOS Var to LY Act",
            ifnull(
                ("CW Cy PCOS Spend (Combo) Act") - ("Ly PCOS Spend Act"), 0
            ) as "CW PCOS Var to LY Act",
            ifnull("Budget (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                0
            ) as "Budget Selected Budget GCAT £ Std",
            ifnull("LE (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)", 0
            ) as "LE Selected Budget GCAT £ Std",
            ifnull("LW (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)", 0
            ) as "LW Selected Budget GCAT £ Std",
            ifnull("CW (Budget) Forecast Gross Margin £ Std", 0) - ifnull(
                "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)", 0
            ) as "CW Selected Budget GCAT £ Std",
            ifnull("Budget (Budget) Forecast Gross Margin £ Act", 0) - ifnull(
                "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
                0
            ) as "Budget Selected Budget GCAT £ Act",
            ifnull("LE (Budget) Forecast Gross Margin £ Act", 0) - ifnull(
                "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)", 0
            ) as "LE Selected Budget GCAT £ Act",
            ifnull("LW (Budget) Forecast Gross Margin £ Act", 0) - ifnull(
                "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)", 0
            ) as "LW Selected Budget GCAT £ Act",
            ifnull("CW (Budget) Forecast Gross Margin £ Act", 0) - ifnull(
                "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)", 0
            ) as "CW Selected Budget GCAT £ Act",
            case
                when
                    nvl(
                        "Budget (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
                        0
                    )
                    = 0
                then 0
                else
                    ifnull(
                        ("Budget CY Total Customer-Invoiced Trade (Combo)") - (
                            "Budget (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
                        )
                        / (
                            "Budget (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
                        ),
                        0
                    )
            end as "Budget Total Customer-Invoiced % vs Budget",
            case
                when
                    nvl(
                        "LE (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
                        0
                    )
                    = 0
                then 0
                else
                    ifnull(
                        ("LE CY Total Customer-Invoiced Trade (Combo)") - (
                            "LE (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
                        )
                        / (
                            "LE (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
                        ),
                        0
                    )
            end as "LE Total Customer-Invoiced % vs Budget",
            case
                when
                    nvl(
                        "LW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
                        0
                    )
                    = 0
                then 0
                else
                    ifnull(
                        ("LW CY Total Customer-Invoiced Trade (Combo)") - (
                            "LW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
                        )
                        / (
                            "LW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
                        ),
                        0
                    )
            end as "LW Total Customer-Invoiced % vs Budget",
            case
                when
                    nvl(
                        "CW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
                        0
                    )
                    = 0
                then 0
                else
                    ifnull(
                        ("CW CY Total Customer-Invoiced Trade (Combo)") - (
                            "CW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
                        )
                        / (
                            "CW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
                        ),
                        0
                    )
            end as "CW Total Customer-Invoiced % vs Budget",

            case
                when
                    nvl("Budget Forecast Total Customer-Invoiced Trade £ (Combo)", 0)
                    = 0
                then 0
                else
                    ifnull(
                        ("Budget CY Total Customer-Invoiced Trade (Combo)")
                        - ("Budget Forecast Total Customer-Invoiced Trade £ (Combo)")
                        / ("Budget Forecast Total Customer-Invoiced Trade £ (Combo)"),
                        0
                    )
            end as "Budget Total Customer-Invoiced % vs Forecast",
            case
                when nvl("LW Forecast Total Customer-Invoiced Trade £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("LW CY Total Customer-Invoiced Trade (Combo)")
                        - ("LW Forecast Total Customer-Invoiced Trade £ (Combo)")
                        / ("LW Forecast Total Customer-Invoiced Trade £ (Combo)"),
                        0
                    )
            end as "LW Total Customer-Invoiced % vs Forecast",
            case
                when nvl("CW Forecast Total Customer-Invoiced Trade £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("CW CY Total Customer-Invoiced Trade (Combo)")
                        - ("CW Forecast Total Customer-Invoiced Trade £ (Combo)")
                        / ("CW Forecast Total Customer-Invoiced Trade £ (Combo)"),
                        0
                    )
            end as "CW Total Customer-Invoiced % vs Forecast",
            case
                when nvl("LE Forecast Total Customer-Invoiced Trade £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("LE CY Total Customer-Invoiced Trade (Combo)")
                        - ("LE Forecast Total Customer-Invoiced Trade £ (Combo)")
                        / ("LE Forecast Total Customer-Invoiced Trade £ (Combo)"),
                        0
                    )
            end as "LE Total Customer-Invoiced % vs Forecast",
            case
                when nvl("LY Total Customer-Invoiced Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        ("Budget CY Total Customer-Invoiced Trade (Combo)")
                        - ("LY Total Customer-Invoiced Trade £")
                        / ("LY Total Customer-Invoiced Trade £"),
                        0
                    )
            end as "Budget Total Customer-Invoiced % YoY",
            case
                when nvl("LY Total Customer-Invoiced Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        ("LW CY Total Customer-Invoiced Trade (Combo)")
                        - ("LY Total Customer-Invoiced Trade £")
                        / ("LY Total Customer-Invoiced Trade £"),
                        0
                    )
            end as "LW Total Customer-Invoiced % YoY",
            case
                when nvl("LY Total Customer-Invoiced Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        ("CW CY Total Customer-Invoiced Trade (Combo)")
                        - ("LY Total Customer-Invoiced Trade £")
                        / ("LY Total Customer-Invoiced Trade £"),
                        0
                    )
            end as "CW Total Customer-Invoiced % YoY",
            case
                when nvl("LY Total Customer-Invoiced Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        ("LE CY Total Customer-Invoiced Trade (Combo)")
                        - ("LY Total Customer-Invoiced Trade £")
                        / ("LY Total Customer-Invoiced Trade £"),
                        0
                    )
            end as "LE Total Customer-Invoiced % YoY",
            ifnull(
                ("Budget CY Total Customer-Invoiced Trade Spend (Combo)")
                - ("(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)"),
                0
            ) as "Budget Total Customer-Invoiced Var to Budget",
            ifnull(
                ("CW CY Total Customer-Invoiced Trade Spend (Combo)")
                - ("(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)"),
                0
            ) as "CW Total Customer-Invoiced Var to Budget",
            ifnull(
                ("LE CY Total Customer-Invoiced Trade Spend (Combo)")
                - ("(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)"),
                0
            ) as "LE Total Customer-Invoiced Var to Budget",
            ifnull(
                ("LW CY Total Customer-Invoiced Trade Spend (Combo)")
                - ("(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)"),
                0
            ) as "LW Total Customer-Invoiced Var to Budget",
            ifnull(
                ("Budget CY Total Customer-Invoiced Trade (Combo)")
                - ("Budget Forecast Total Customer-Invoiced Trade £ (Combo)"),
                0
            )
            * -1 as "Budget Total Customer-Invoiced Var to Forecast",
            ifnull(
                ("LW CY Total Customer-Invoiced Trade (Combo)")
                - ("LW Forecast Total Customer-Invoiced Trade £ (Combo)"),
                0
            )
            * -1 as "LW Total Customer-Invoiced Var to Forecast",
            ifnull(
                ("LE CY Total Customer-Invoiced Trade (Combo)")
                - ("LE Forecast Total Customer-Invoiced Trade £ (Combo)"),
                0
            )
            * -1 as "LE Total Customer-Invoiced Var to Forecast",
            ifnull(
                ("CW CY Total Customer-Invoiced Trade (Combo)")
                - ("CW Forecast Total Customer-Invoiced Trade £ (Combo)"),
                0
            )
            * -1 as "CW Total Customer-Invoiced Var to Forecast",
            ifnull(
                "Budget CY Total Customer-Invoiced Trade Spend (Combo)"
                - "LY Total Customer-Invoiced Trade Spend",
                0
            ) as "Budget Total Customer-Invoiced Var to LY",
            ifnull(
                "CW CY Total Customer-Invoiced Trade Spend (Combo)"
                - "LY Total Customer-Invoiced Trade Spend",
                0
            ) as "CW Total Customer-Invoiced Var to LY",
            ifnull(
                "LE CY Total Customer-Invoiced Trade Spend (Combo)"
                - "LY Total Customer-Invoiced Trade Spend",
                0
            ) as "LE Total Customer-Invoiced Var to LY",
            ifnull(
                "LW CY Total Customer-Invoiced Trade Spend (Combo)"
                - "LY Total Customer-Invoiced Trade Spend",
                0
            ) as "LW Total Customer-Invoiced Var to LY",
            case
                when nvl("Budget Budget Variable Trade £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("Budget Cy Variable Trade Spend")
                        - ("Budget Budget Variable Trade £ (Combo)")
                        / (- "Budget Budget Variable Trade £ (Combo)"),
                        0
                    )
            end as "Budget Variable Trade % vs Budget",
            case
                when nvl("CW Budget Variable Trade £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("CW Cy Variable Trade Spend")
                        - ("CW Budget Variable Trade £ (Combo)")
                        / (- "CW Budget Variable Trade £ (Combo)"),
                        0
                    )
            end as "CW Variable Trade % vs Budget",
            case
                when nvl("LE Budget Variable Trade £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("LE Cy Variable Trade Spend")
                        - ("LE Budget Variable Trade £ (Combo)")
                        / (- "LE Budget Variable Trade £ (Combo)"),
                        0
                    )
            end as "LE Variable Trade % vs Budget",
            case
                when nvl("LW Budget Variable Trade £ (Combo)", 0) = 0
                then 0
                else
                    ifnull(
                        ("LW Cy Variable Trade Spend")
                        - ("LW Budget Variable Trade £ (Combo)")
                        / (- "LW Budget Variable Trade £ (Combo)"),
                        0
                    )
            end as "LW Variable Trade % vs Budget",
            case
                when nvl("Budget Forecast Variable Trade Spend", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("Budget Cy Variable Trade Spend")
                            - ("Budget Forecast Variable Trade Spend")
                        )
                        / ("Budget Forecast Variable Trade Spend"),
                        0
                    )
            end as "Budget Variable Trade % vs Forecast",
            case
                when nvl("LW Forecast Variable Trade Spend", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LW Cy Variable Trade Spend")
                            - ("LW Forecast Variable Trade Spend")
                        )
                        / ("LW Forecast Variable Trade Spend"),
                        0
                    )
            end as "LW Variable Trade % vs Forecast",
            case
                when nvl("LE Forecast Variable Trade Spend", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LE Cy Variable Trade Spend")
                            - ("LE Forecast Variable Trade Spend")
                        )
                        / ("LE Forecast Variable Trade Spend"),
                        0
                    )
            end as "LE Variable Trade % vs Forecast",
            case
                when nvl("CW Forecast Variable Trade Spend", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("CW Cy Variable Trade Spend")
                            - ("CW Forecast Variable Trade Spend")
                        )
                        / ("CW Forecast Variable Trade Spend"),
                        0
                    )
            end as "CW Variable Trade % vs Forecast",
            case
                when nvl("LY Total Variable Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("Budget CY Total Variable Trade £ (Combo)")
                            - ("LY Total Variable Trade £")
                        )
                        / ("LY Total Variable Trade £"),
                        0
                    )
            end as "Budget Variable Trade % YoY",
            case
                when nvl("LY Total Variable Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("CW CY Total Variable Trade £ (Combo)")
                            - ("LY Total Variable Trade £")
                        )
                        / ("LY Total Variable Trade £"),
                        0
                    )
            end as "CW Variable Trade % YoY",
            case
                when nvl("LY Total Variable Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LE CY Total Variable Trade £ (Combo)")
                            - ("LY Total Variable Trade £")
                        )
                        / ("LY Total Variable Trade £"),
                        0
                    )
            end as "LE Variable Trade % YoY",
            case
                when nvl("LY Total Variable Trade £", 0) = 0
                then 0
                else
                    ifnull(
                        (
                            ("LW CY Total Variable Trade £ (Combo)")
                            - ("LY Total Variable Trade £")
                        )
                        / ("LY Total Variable Trade £"),
                        0
                    )
            end as "LW Variable Trade % YoY",
            (ifnull("Budget Cy Variable Trade Spend", 0)) - (
                ifnull("Budget Variable Trade Spend", 0)
            ) as "Budget Variable Trade vs Budget",
            (ifnull("CW Cy Variable Trade Spend", 0)) - (
                ifnull("Budget Variable Trade Spend", 0)
            ) as "CW Variable Trade vs Budget",
            (ifnull("LW Cy Variable Trade Spend", 0)) - (
                ifnull("Budget Variable Trade Spend", 0)
            ) as "LW Variable Trade vs Budget",
            (ifnull("LE Cy Variable Trade Spend", 0)) - (
                ifnull("Budget Variable Trade Spend", 0)
            ) as "LE Variable Trade vs Budget",
            (ifnull("Budget Cy Variable Trade Spend", 0)) - (
                ifnull("Budget Forecast Variable Trade Spend", 0)
            ) as "Budget Variable Trade vs Forecast",
            (ifnull("LW Cy Variable Trade Spend", 0)) - (
                ifnull("LW Forecast Variable Trade Spend", 0)
            ) as "LW Variable Trade vs Forecast",
            (ifnull("CW Cy Variable Trade Spend", 0)) - (
                ifnull("CW Forecast Variable Trade Spend", 0)
            ) as "CW Variable Trade vs Forecast",
            (ifnull("LE Cy Variable Trade Spend", 0)) - (
                ifnull("LE Forecast Variable Trade Spend", 0)
            ) as "LE Variable Trade vs Forecast",

            (ifnull("Budget Cy Variable Trade Spend", 0))
            - (ifnull("Ly Variable Trade Spend", 0)) as "Budget Variable Trade vs Ly",
            (ifnull("LW Cy Variable Trade Spend", 0))
            - (ifnull("Ly Variable Trade Spend", 0)) as "LW Variable Trade vs Ly",
            (ifnull("CW Cy Variable Trade Spend", 0))
            - (ifnull("Ly Variable Trade Spend", 0)) as "CW Variable Trade vs Ly",
            (ifnull("LE Cy Variable Trade Spend", 0))
            - (ifnull("Ly Variable Trade Spend", 0)) as "LE Variable Trade vs Ly",
            ifnull("CY Net Sales £", 0)
            + ifnull("CY Total Customer-Invoiced Trade £", 0) as "CY NRR £"

        from pre_processing main
        left join uber_budget on uber_budget.budget_unique_key = main.unique_key
        left join uber_current_week on uber_current_week.cw_unique_key = main.unique_key
        left join uber_last_week on uber_last_week.lw_unique_key = main.unique_key
        left join uber_le on uber_le.le_unique_key = main.unique_key
    )

select
    cast(substring("UNIQUE_KEY", 1, 32) as text(32)) as "Unique Key",
    cast(substring("Branding", 1, 255) as text(255)) as "Branding",
    cast(substring("Product Class", 1, 255) as text(255)) as "Product Class",
    cast(substring("Sub Product", 1, 255) as text(255)) as "Sub Product",
    cast(substring("Packaging Size", 1, 255) as text(255)) as "Packaging Size",
    cast(substring("MARKET", 1, 255) as text(255)) as "Market",
    cast(substring("SUBMARKET", 1, 255) as text(255)) as "Sub Market",
    cast(substring("TRADE_CLASS", 1, 255) as text(255)) as "Trade Class",
    cast(substring("TRADE_GROUP", 1, 255) as text(255)) as "Trade Group",
    cast(substring("TRADE_TYPE", 1, 255) as text(255)) as "Trade Type",
    cast(substring("TRADE_SECTOR", 1, 255) as text(255)) as "Trade Sector",
    cast("Budget PCOS Var Flag" as number(18, 5)) as "Budget PCOS Var Flag",
    cast("calendar date" as timestamp_ntz(9)) as "Calendar date",
    cast("Report fiscal year" as number(38, 0)) as "Report fiscal year",
    cast(
        "Report fiscal year period no" as number(38, 0)
    ) as "Report fiscal year period no",
    cast("Snapshot forecast date" as date) as "Snapshot forecast date",
    cast(substring("Frozen forecast", 1, 255) as text(255)) as "Frozen forecast",
    cast(
        "Frozen forecast delineation date" as date
    ) as "Frozen forecast delineation date",
    cast(
        "Forecast Customer-Invoiced Fixed Trade £" as float
    ) as "Forecast Customer-Invoiced Fixed Trade £",
    cast(
        "Forecast Total Variable Trade £" as float
    ) as "Forecast total variable trade £",

    cast("Cy Std Bought-In Amt £" as number(38, 10)) as "Cy Std Bought-In Amt £",
    cast("Cy Std Co-Packing Amt £" as number(38, 10)) as "Cy Std Co-Packing Amt £",
    cast("Cy Std Ingredient Amt £" as number(38, 10)) as "Cy Std Ingredient Amt £",
    cast("Cy Std Labour Amt £" as number(38, 10)) as "Cy Std Labour Amt £",
    cast("Cy Std Other Amt £" as number(38, 10)) as "Cy Std Other Amt £",
    cast("Cy Std Packaging Amt £" as number(38, 10)) as "Cy Std Packaging Amt £",
    cast(
        "Cy Var to Std Bought-In Amt £" as number(38, 10)
    ) as "Cy Var to Std Bought-In Amt £",
    cast(
        "Cy Var to Std Co-Packing Amt £" as number(38, 10)
    ) as "Cy Var to Std Co-Packing Amt £",
    cast(
        "Cy Var to Std Ingredients Amt £" as number(38, 10)
    ) as "Cy Var to Std Ingredients Amt £",
    cast(
        "Cy Var to Std Labour Amt £" as number(38, 10)
    ) as "Cy Var to Std Labour Amt £",
    cast("Cy Var to Std Other Amt £" as number(38, 10)) as "Cy Var to Std Other Amt £",
    cast(
        "Cy Var to Std Packaging Amt £" as number(38, 10)
    ) as "Cy Var to Std Packaging Amt £",
    cast(
        "Cy Despatched Case Quantity" as number(38, 10)
    ) as "Cy Despatched Case Quantity",
    cast(
        "CY Despatched Consumer Unit Quantity" as number(38, 10)
    ) as "CY Despatched Consumer Unit Quantity",
    cast("Cy Despatched Kg Quantity" as number(38, 10)) as "Cy Despatched Kg Quantity",
    cast(
        "CY Despatched Packet Quantity" as number(38, 10)
    ) as "CY Despatched Packet Quantity",
    cast(
        "Cy Despatched Pallet Quantity" as number(38, 10)
    ) as "Cy Despatched Pallet Quantity",
    cast(
        "Ly Despatched Case Quantity" as number(38, 10)
    ) as "Ly Despatched Case Quantity",
    cast(
        "LY Despatched Consumer Unit Quantity" as number(38, 10)
    ) as "LY Despatched Consumer Unit Quantity",
    cast("Ly Despatched Kg Quantity" as number(38, 10)) as "Ly Despatched Kg Quantity",
    cast(
        "LY Despatched Packet Quantity" as number(38, 10)
    ) as "LY Despatched Packet Quantity",
    cast(
        "Ly Despatched Pallet Quantity" as number(38, 10)
    ) as "Ly Despatched Pallet Quantity",
    cast("Cy Gross Amount £" as number(38, 10)) as "Cy Gross Amount £",
    cast("Cy Invoice Gross Amount £" as number(38, 10)) as "Cy Invoice Gross Amount £",
    cast("Ly Gross Amount £" as number(38, 10)) as "Ly Gross Amount £",
    cast("Cy Category £" as number(38, 10)) as "Cy Category £",
    cast(
        "Cy Direct Shopper Marking £" as number(38, 10)
    ) as "Cy Direct Shopper Marking £",
    cast(
        "Cy Early Settlement Discount £" as number(38, 10)
    ) as "Cy Early Settlement Discount £",
    cast("Cy EDLP £" as number(38, 10)) as "Cy EDLP £",
    cast("Cy Field Marketing £" as number(38, 10)) as "Cy Field Marketing £",
    cast("Cy Fixed Annual Pymts £" as number(38, 10)) as "Cy Fixed Annual Pymts £",
    cast("Cy Growth Incentives £" as number(38, 10)) as "Cy Growth Incentives £",
    cast(
        "Cy Indirect Shopper Marking £" as number(38, 10)
    ) as "Cy Indirect Shopper Marking £",
    cast("Cy Other Direct Pymts £" as number(38, 10)) as "Cy Other Direct Pymts £",
    cast("Cy Other Indirect Pymts £" as number(38, 10)) as "Cy Other Indirect Pymts £",
    cast("Cy Other Trade £" as number(38, 10)) as "Cy Other Trade £",
    cast("Cy Promo Fixed Funding £" as number(38, 10)) as "Cy Promo Fixed Funding £",
    cast(
        "Cy Range Support Allowance £" as number(38, 10)
    ) as "Cy Range Support Allowance £",
    cast("Cy Retro £" as number(38, 10)) as "Cy Retro £",
    cast("Forecast Added Value Pack £" as float) as "Forecast Added Value Pack £",
    cast(
        "Forecast Early Settlement Discount £" as float
    ) as "Forecast Early Settlement Discount £",
    cast("Forecast EDLP £" as float) as "Forecast EDLP £",
    cast("Forecast Gross Amount £" as float) as "Forecast Gross Amount £",
    cast("Forecast Growth Incentives £" as float) as "Forecast Growth Incentives £",
    cast("Forecast Net Sales £" as float) as "Forecast Net Sales £",
    cast("Forecast PCOS (Std) £" as float) as "Forecast PCOS (Std) £",
    cast("Forecast PCOS (Var) £" as float) as "Forecast PCOS (Var) £",
    cast("Forecast Range Support £" as float) as "Forecast Range Support £",
    cast("Forecast RSA Incentives £" as float) as "Forecast RSA Incentives £",
    cast(
        "Forecast Total Customer-Invoiced Trade £" as float
    ) as "Forecast Total Customer-Invoiced Trade £",
    cast(
        "Forecast Total Non-Customer Invoiced Fixed Trade £" as float
    ) as "Forecast Total Non-Customer Invoiced Fixed Trade £",
    cast("Forecast Total Volume Case" as float) as "Forecast Total Volume Case",
    cast(
        "Forecast Total Volume Consumer Units" as float
    ) as "Forecast Total Volume Consumer Units",
    cast("Forecast Total Volume Kg" as float) as "Forecast Total Volume Kg",
    cast("Forecast Total Volume Packet" as float) as "Forecast Total Volume Packet",
    cast("Forecast Total Volume Pallet" as float) as "Forecast Total Volume Pallet",
    cast("Budget Gross Amount £" as number(38, 10)) as "Budget Gross Amount £",
    cast("Ly Std Bought-In Amt £" as number(38, 10)) as "Ly Std Bought-In Amt £",
    cast("Ly Std Co-Packing Amt £" as number(38, 10)) as "Ly Std Co-Packing Amt £",
    cast("Ly Std Ingredient Amt £" as number(38, 10)) as "Ly Std Ingredient Amt £",
    cast("Ly Std Labour Amt £" as number(38, 10)) as "Ly Std Labour Amt £",
    cast("Ly Std Other Amt £" as number(38, 10)) as "Ly Std Other Amt £",
    cast("Ly Std Packaging Amt £" as number(38, 10)) as "Ly Std Packaging Amt £",
    cast(
        "Ly Var to Std Bought-In Amt £" as number(38, 10)
    ) as "Ly Var to Std Bought-In Amt £",
    cast(
        "Ly Var to Std Co-Packing Amt  £" as number(38, 10)
    ) as "Ly Var to Std Co-Packing Amt  £",
    cast(
        "Ly Var to Std Ingredients Amt  £" as number(38, 10)
    ) as "Ly Var to Std Ingredients Amt  £",
    cast(
        "Ly Var to Std Labour Amt  £" as number(38, 10)
    ) as "Ly Var to Std Labour Amt  £",
    cast(
        "Ly Var to Std Other Amt  £" as number(38, 10)
    ) as "Ly Var to Std Other Amt  £",
    cast(
        "Ly Var to Std Packaging Amt  £" as number(38, 10)
    ) as "Ly Var to Std Packaging Amt  £",
    cast("Ly Invoice Gross Amount £" as number(38, 10)) as "Ly Invoice Gross Amount £",
    cast("Cy AVP Discount £" as number(38, 10)) as "Cy AVP Discount £",
    cast("Ly AVP Discount £" as number(38, 10)) as "Ly AVP Discount £",
    cast("Ly Category £" as number(38, 10)) as "Ly Category £",
    cast(
        "Ly Direct Shopper Marking £" as number(38, 10)
    ) as "Ly Direct Shopper Marking £",
    cast(
        "Ly Early Settlement Discount £" as number(38, 10)
    ) as "Ly Early Settlement Discount £",
    cast("Ly EDLP £" as number(38, 10)) as "Ly EDLP £",
    cast("Ly Field Marketing £" as number(38, 10)) as "Ly Field Marketing £",
    cast("Ly Fixed Annual Pymts £" as number(38, 10)) as "Ly Fixed Annual Pymts £",
    cast("Ly Growth Incentives £" as number(38, 10)) as "Ly Growth Incentives £",
    cast(
        "Ly Indirect Shopper Marking £" as number(38, 10)
    ) as "Ly Indirect Shopper Marking £",
    cast("Ly Other Direct Pymts £" as number(38, 10)) as "Ly Other Direct Pymts £",
    cast("Ly Other Indirect Pymts £" as number(38, 10)) as "Ly Other Indirect Pymts £",
    cast("Ly Other Trade £" as number(38, 10)) as "Ly Other Trade £",
    cast("Ly Promo Fixed Funding £" as number(38, 10)) as "Ly Promo Fixed Funding £",
    cast(
        "Ly Range Support Allowance £" as number(38, 10)
    ) as "Ly Range Support Allowance £",
    cast("Ly Retro £" as number(38, 10)) as "Ly Retro £",
    cast(
        "(Budget) Forecast Gross Amount (Combo)" as float
    ) as "(Budget) Forecast Gross Amount (Combo)",
    cast(
        "(Budget) Forecast Gross Sales £ (Combo)" as float
    ) as "(Budget) Forecast Gross Sales £ (Combo)",
    cast("Budget Variable Trade Spend" as float) as "Budget Variable Trade Spend",
    cast("CY Added Value Pack £" as number(38, 10)) as "CY Added Value Pack £",
    cast(
        "CY Customer-Invoiced Fixed Trade £" as number(38, 10)
    ) as "CY Customer-Invoiced Fixed Trade £",
    cast("CY Gross Sales £" as number(38, 10)) as "CY Gross Sales £",
    cast("CY Total Std PCOS Amt £" as number(38, 10)) as "CY Total Std PCOS Amt £",
    cast(
        "CY Total Var to Std PCOS Amt £" as number(38, 10)
    ) as "CY Total Var to Std PCOS Amt £",
    cast("CY Permanent Discounts £" as number(38, 10)) as "CY Permanent Discounts £",
    cast(
        "CY Total Non-Customer Invoiced Fixed Trade £" as number(38, 10)
    ) as "CY Total Non-Customer Invoiced Fixed Trade £",
    cast("CY Total Variable Trade £" as number(38, 10)) as "CY Total Variable Trade £",
    cast(
        "Forecast 1 Gross Amount £ (Combo)" as float
    ) as "Forecast 1 Gross Amount £ (Combo)",
    cast("Forecast Permanent Discounts £" as float) as "Forecast Permanent Discounts £",
    cast(
        "Forecast Total Standard PCOS Amt £" as float
    ) as "Forecast Total Standard PCOS Amt £",
    cast("Forecast Total Var PCOS Amt £" as float) as "Forecast Total Var PCOS Amt £",
    cast("LY Gross Sales £" as number(38, 10)) as "LY Gross Sales £",
    cast("LY Permanent Discounts £" as number(38, 10)) as "LY Permanent Discounts £",
    cast(
        "LY Total Customer-Invoiced Trade £" as number(38, 10)
    ) as "LY Total Customer-Invoiced Trade £",
    cast(
        "LY Total Non-Customer Invoiced Fixed Trade £" as number(38, 10)
    ) as "LY Total Non-Customer Invoiced Fixed Trade £",
    cast("LY Total Std PCOS Amt £" as number(38, 10)) as "LY Total Std PCOS Amt £",
    cast(
        "LY Total Var to Std PCOS Amt £" as number(38, 10)
    ) as "LY Total Var to Std PCOS Amt £",
    cast("LY Total Variable Trade £" as number(38, 10)) as "LY Total Variable Trade £",
    cast(
        "LY Fixed Trade Customer-Invoiced (Combo Split)" as number(38, 10)
    ) as "LY Fixed Trade Customer-Invoiced (Combo Split)",
    cast("CY Total PCOS Amt £" as number(38, 10)) as "CY Total PCOS Amt £",
    cast(
        "CY Total Customer-Invoiced Trade £" as number(38, 10)
    ) as "CY Total Customer-Invoiced Trade £",
    cast(
        "Budget (Budget) Forecast Added Value Pack (Combo)" as float
    ) as "Budget (Budget) Forecast Added Value Pack (Combo)",
    cast(
        "Budget (Budget) Forecast Gross Amount (Combo)" as float
    ) as "Budget (Budget) Forecast Gross Amount (Combo)",
    cast(
        "Budget (Budget) Forecast Gross Sales £ (Combo)" as float
    ) as "Budget (Budget) Forecast Gross Sales £ (Combo)",
    cast(
        "Budget CY Added Value Pack £" as number(38, 10)
    ) as "Budget CY Added Value Pack £",
    cast(
        "Budget CY Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "Budget CY Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "Budget (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)" as float
    ) as "Budget (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
    cast(
        "Budget Cy PCOS Spend (Combo) Std" as float
    ) as "Budget Cy PCOS Spend (Combo) Std",
    cast(
        "Budget Cy PCOS Spend (Combo) Act" as float
    ) as "Budget Cy PCOS Spend (Combo) Act",
    cast(
        "Budget (Budget) Forecast Net Sales £ (Combo)" as float
    ) as "Budget (Budget) Forecast Net Sales £ (Combo)",
    cast(
        "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "Budget (Budget) Forecast NRR £ (Combo)" as float
    ) as "Budget (Budget) Forecast NRR £ (Combo)",
    cast(
        "Budget (Budget) Forecast Permanent Discounts £ (Combo)" as float
    ) as "Budget (Budget) Forecast Permanent Discounts £ (Combo)",
    cast(
        "Budget (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "Budget (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "Budget (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "Budget (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "Budget Budget Variable Trade £ (Combo)" as float
    ) as "Budget Budget Variable Trade £ (Combo)",
    cast(
        "Budget CY Customer-Invoiced Variable Trade £ (Combo Split)" as float
    ) as "Budget CY Customer-Invoiced Variable Trade £ (Combo Split)",
    cast(
        "Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "Budget CY Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "Budget CY Non-Customer Invoiced Fixed Trade Spend (Combo)" as float
    ) as "Budget CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
    cast(
        "Budget CY Total Customer-Invoiced Trade (Combo)" as float
    ) as "Budget CY Total Customer-Invoiced Trade (Combo)",
    cast(
        "Budget CY Total Variable Trade £ (Combo)" as float
    ) as "Budget CY Total Variable Trade £ (Combo)",
    cast(
        "Budget Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "Budget Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "Budget Forecast PCOS £ (Combo) Act" as float
    ) as "Budget Forecast PCOS £ (Combo) Act",
    cast(
        "Budget Forecast PCOS £ (Combo) Std" as float
    ) as "Budget Forecast PCOS £ (Combo) Std",
    cast(
        "Budget Forecast Permanent Discounts £ (Combo)" as float
    ) as "Budget Forecast Permanent Discounts £ (Combo)",
    cast(
        "Budget (Budget) Forecast Gross Margin £ Act" as float
    ) as "Budget (Budget) Forecast Gross Margin £ Act",
    cast(
        "Budget (Budget) Forecast Gross Margin £ Std" as float
    ) as "Budget (Budget) Forecast Gross Margin £ Std",
    cast(
        "Budget (Budget) Forecast Total PCOS Amt £ Std PCOS" as float
    ) as "Budget (Budget) Forecast Total PCOS Amt £ Std PCOS",
    cast(
        "Budget Budget Fixed Trade Spend" as float
    ) as "Budget Budget Fixed Trade Spend",
    cast("Budget Budget PCOS Spend Act" as float) as "Budget Budget PCOS Spend Act",
    cast("Budget Budget PCOS Spend Std" as float) as "Budget Budget PCOS Spend Std",
    cast(
        "Budget Customer-Invoiced Variable Trade % vs Budget (Split)" as float
    ) as "Budget Customer-Invoiced Variable Trade % vs Budget (Split)",
    cast(
        "Budget Customer-Invoiced Variable Trade % vs Forecast (Split)" as float
    ) as "Budget Customer-Invoiced Variable Trade % vs Forecast (Split)",
    cast(
        "Budget Customer-Invoiced Variable Trade % vs LY (Split)" as float
    ) as "Budget Customer-Invoiced Variable Trade % vs LY (Split)",
    cast(
        "Budget Customer-Invoiced Variable Trade Var to Budget (Split)" as float
    ) as "Budget Customer-Invoiced Variable Trade Var to Budget (Split)",
    cast(
        "Budget Customer-Invoiced Variable Trade Var to Forecast (Split)" as float
    ) as "Budget Customer-Invoiced Variable Trade Var to Forecast (Split)",
    cast(
        "Budget Customer-Invoiced Variable Trade Var to LY (Split)" as float
    ) as "Budget Customer-Invoiced Variable Trade Var to LY (Split)",
    cast("Budget CY NRR £ (Combo)" as float) as "Budget CY NRR £ (Combo)",
    cast("Budget Cy Variable Trade Spend" as float) as "Budget Cy Variable Trade Spend",
    cast(
        "Budget Forecast 1 Permanent Discounts £ (Combo)" as float
    ) as "Budget Forecast 1 Permanent Discounts £ (Combo)",
    cast(
        "Budget Forecast Added Value Pack (Combo)" as float
    ) as "Budget Forecast Added Value Pack (Combo)",
    cast(
        "Budget Forecast Fixed Trade £ (Combo)" as float
    ) as "Budget Forecast Fixed Trade £ (Combo)",
    cast(
        "Budget Forecast Fixed Trade Customer-Invoiced" as float
    ) as "Budget Forecast Fixed Trade Customer-Invoiced",
    cast(
        "Budget Forecast Fixed Trade Spend" as float
    ) as "Budget Forecast Fixed Trade Spend",
    cast(
        "Budget Forecast GCAT £ (Combo) Act" as float
    ) as "Budget Forecast GCAT £ (Combo) Act",
    cast(
        "Budget Forecast GCAT £ (Combo) Std" as float
    ) as "Budget Forecast GCAT £ (Combo) Std",
    cast(
        "Budget Forecast Gross Amount (Combo)" as float
    ) as "Budget Forecast Gross Amount (Combo)",
    cast(
        "Budget Forecast Net Sales £ (Combo)" as float
    ) as "Budget Forecast Net Sales £ (Combo)",
    cast(
        "Budget Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "Budget Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "Budget Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "Budget Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "Budget Forecast Variable Trade £ (Combo) (1)" as float
    ) as "Budget Forecast Variable Trade £ (Combo) (1)",
    cast(
        "Budget Forecast Variable Trade Spend" as float
    ) as "Budget Forecast Variable Trade Spend",
    cast("Budget LY NRR £ (Combo)" as float) as "Budget LY NRR £ (Combo)",
    cast(
        "Budget LY Total Customer-Invoiced Trade (Combo)" as float
    ) as "Budget LY Total Customer-Invoiced Trade (Combo)",
    cast(
        "Budget Abs Var Forecast Cust-Inv Trade" as float
    ) as "Budget Abs Var Forecast Cust-Inv Trade",
    cast(
        "Budget Abs Var Forecast GCAT Std" as float
    ) as "Budget Abs Var Forecast GCAT Std",
    cast("Budget Abs Var Forecast NRR" as float) as "Budget Abs Var Forecast NRR",
    cast(
        "Budget Abs Var Forecast GCAT Act" as float
    ) as "Budget Abs Var Forecast GCAT Act",
    cast("Budget CY GCAT £ (Combo) Act" as float) as "Budget CY GCAT £ (Combo) Act",
    cast("Budget CY GCAT £ (Combo) Std" as float) as "Budget CY GCAT £ (Combo) Std",
    cast("Budget Forecast NRR £ (Combo)" as float) as "Budget Forecast NRR £ (Combo)",
    cast(
        "LE (Budget) Forecast Added Value Pack (Combo)" as float
    ) as "LE (Budget) Forecast Added Value Pack (Combo)",
    cast(
        "LE Budget (Budget) Forecast Gross Amount (Combo)" as float
    ) as "LE Budget (Budget) Forecast Gross Amount (Combo)",
    cast(
        "LE (Budget) Forecast Gross Sales £ (Combo)" as float
    ) as "LE (Budget) Forecast Gross Sales £ (Combo)",
    cast("LE CY Added Value Pack £" as number(38, 10)) as "LE CY Added Value Pack £",
    cast(
        "LE CY Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "LE CY Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "LE (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)" as float
    ) as "LE (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
    cast("LE Cy PCOS Spend (Combo) Std" as float) as "LE Cy PCOS Spend (Combo) Std",
    cast("LE Cy PCOS Spend (Combo) Act" as float) as "LE Cy PCOS Spend (Combo) Act",
    cast(
        "LE (Budget) Forecast Net Sales £ (Combo)" as float
    ) as "LE (Budget) Forecast Net Sales £ (Combo)",
    cast(
        "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "LE (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "LE (Budget) Forecast NRR £ (Combo)" as float
    ) as "LE (Budget) Forecast NRR £ (Combo)",
    cast(
        "LE (Budget) Forecast Permanent Discounts £ (Combo)" as float
    ) as "LE (Budget) Forecast Permanent Discounts £ (Combo)",
    cast(
        "LE (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "LE (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "LE (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "LE (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "LE Budget Variable Trade £ (Combo)" as float
    ) as "LE Budget Variable Trade £ (Combo)",
    cast(
        "LE CY Customer-Invoiced Variable Trade £ (Combo Split)" as float
    ) as "LE CY Customer-Invoiced Variable Trade £ (Combo Split)",
    cast(
        "LE CY Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "LE CY Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "LE CY Non-Customer Invoiced Fixed Trade Spend (Combo)" as float
    ) as "LE CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
    cast(
        "LE CY Total Customer-Invoiced Trade (Combo)" as float
    ) as "LE CY Total Customer-Invoiced Trade (Combo)",
    cast(
        "LE CY Total Variable Trade £ (Combo)" as float
    ) as "LE CY Total Variable Trade £ (Combo)",
    cast(
        "LE Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "LE Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast("LE Forecast PCOS £ (Combo) Act" as float) as "LE Forecast PCOS £ (Combo) Act",
    cast("LE Forecast PCOS £ (Combo) Std" as float) as "LE Forecast PCOS £ (Combo) Std",
    cast(
        "LE Forecast Permanent Discounts £ (Combo)" as float
    ) as "LE Forecast Permanent Discounts £ (Combo)",
    cast(
        "LE (Budget) Forecast Gross Margin £ Act" as float
    ) as "LE (Budget) Forecast Gross Margin £ Act",
    cast(
        "LE (Budget) Forecast Gross Margin £ Std" as float
    ) as "LE (Budget) Forecast Gross Margin £ Std",
    cast(
        "LE (Budget) Forecast Total PCOS Amt £ Std PCOS" as float
    ) as "LE (Budget) Forecast Total PCOS Amt £ Std PCOS",
    cast("LE Budget Fixed Trade Spend" as float) as "LE Budget Fixed Trade Spend",
    cast("LE Budget PCOS Spend Act" as float) as "LE Budget PCOS Spend Act",
    cast("LE Budget PCOS Spend Std" as float) as "LE Budget PCOS Spend Std",
    cast(
        "LE Customer-Invoiced Variable Trade % vs Budget (Split)" as float
    ) as "LE Customer-Invoiced Variable Trade % vs Budget (Split)",
    cast(
        "LE Customer-Invoiced Variable Trade % vs Forecast (Split)" as float
    ) as "LE Customer-Invoiced Variable Trade % vs Forecast (Split)",
    cast(
        "LE Customer-Invoiced Variable Trade % vs LY (Split)" as float
    ) as "LE Customer-Invoiced Variable Trade % vs LY (Split)",
    cast(
        "LE Customer-Invoiced Variable Trade Var to Budget (Split)" as float
    ) as "LE Customer-Invoiced Variable Trade Var to Budget (Split)",
    cast(
        "LE Customer-Invoiced Variable Trade Var to Forecast (Split)" as float
    ) as "LE Customer-Invoiced Variable Trade Var to Forecast (Split)",
    cast(
        "LE Customer-Invoiced Variable Trade Var to LY (Split)" as float
    ) as "LE Customer-Invoiced Variable Trade Var to LY (Split)",
    cast("LE CY NRR £ (Combo)" as float) as "LE CY NRR £ (Combo)",
    cast("LE Cy Variable Trade Spend" as float) as "LE Cy Variable Trade Spend",
    cast(
        "LE Forecast 1 Permanent Discounts £ (Combo)" as float
    ) as "LE Forecast 1 Permanent Discounts £ (Combo)",
    cast(
        "LE Forecast Added Value Pack (Combo)" as float
    ) as "LE Forecast Added Value Pack (Combo)",
    cast(
        "LE Forecast Fixed Trade £ (Combo)" as float
    ) as "LE Forecast Fixed Trade £ (Combo)",
    cast(
        "LE Forecast Fixed Trade Customer-Invoiced" as float
    ) as "LE Forecast Fixed Trade Customer-Invoiced",
    cast("LE Forecast Fixed Trade Spend" as float) as "LE Forecast Fixed Trade Spend",
    cast("LE Forecast GCAT £ (Combo) Act" as float) as "LE Forecast GCAT £ (Combo) Act",
    cast("LE Forecast GCAT £ (Combo) Std" as float) as "LE Forecast GCAT £ (Combo) Std",
    cast(
        "LE Forecast Gross Amount (Combo)" as float
    ) as "LE Forecast Gross Amount (Combo)",
    cast(
        "LE Forecast Net Sales £ (Combo)" as float
    ) as "LE Forecast Net Sales £ (Combo)",
    cast(
        "LE Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "LE Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "LE Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "LE Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "LE Forecast Variable Trade £ (Combo) (1)" as float
    ) as "LE Forecast Variable Trade £ (Combo) (1)",
    cast(
        "LE Forecast Variable Trade Spend" as float
    ) as "LE Forecast Variable Trade Spend",
    cast("LE LY NRR £ (Combo)" as float) as "LE LY NRR £ (Combo)",
    cast(
        "LE LY Total Customer-Invoiced Trade (Combo)" as float
    ) as "LE LY Total Customer-Invoiced Trade (Combo)",
    cast(
        "LE Abs Var Forecast Cust-Inv Trade" as float
    ) as "LE Abs Var Forecast Cust-Inv Trade",
    cast("LE Abs Var Forecast GCAT Std" as float) as "LE Abs Var Forecast GCAT Std",
    cast("LE Abs Var Forecast NRR" as float) as "LE Abs Var Forecast NRR",
    cast("LE Abs Var Forecast GCAT Act" as float) as "LE Abs Var Forecast GCAT Act",
    cast("LE CY GCAT £ (Combo) Act" as float) as "LE CY GCAT £ (Combo) Act",
    cast("LE CY GCAT £ (Combo) Std" as float) as "LE CY GCAT £ (Combo) Std",
    cast("LE Forecast NRR £ (Combo)" as float) as "LE Forecast NRR £ (Combo)",
    cast(
        "LW (Budget) Forecast Added Value Pack (Combo)" as float
    ) as "LW (Budget) Forecast Added Value Pack (Combo)",
    cast(
        "LW Budget (Budget) Forecast Gross Amount (Combo)" as float
    ) as "LW Budget (Budget) Forecast Gross Amount (Combo)",
    cast(
        "LW (Budget) Forecast Gross Sales £ (Combo)" as float
    ) as "LW (Budget) Forecast Gross Sales £ (Combo)",
    cast("LW CY Added Value Pack £" as number(38, 10)) as "LW CY Added Value Pack £",
    cast(
        "LW CY Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "LW CY Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "LW (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)" as float
    ) as "LW (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
    cast("LW Cy PCOS Spend (Combo) Std" as float) as "LW Cy PCOS Spend (Combo) Std",
    cast("LW Cy PCOS Spend (Combo) Act" as float) as "LW Cy PCOS Spend (Combo) Act",
    cast(
        "LW (Budget) Forecast Net Sales £ (Combo)" as float
    ) as "LW (Budget) Forecast Net Sales £ (Combo)",
    cast(
        "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "LW (Budget) Forecast NRR £ (Combo)" as float
    ) as "LW (Budget) Forecast NRR £ (Combo)",
    cast(
        "LW (Budget) Forecast Permanent Discounts £ (Combo)" as float
    ) as "LW (Budget) Forecast Permanent Discounts £ (Combo)",
    cast(
        "LW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "LW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "LW (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "LW (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "LW Budget Variable Trade £ (Combo)" as float
    ) as "LW Budget Variable Trade £ (Combo)",
    cast(
        "LW CY Customer-Invoiced Variable Trade £ (Combo Split)" as float
    ) as "LW CY Customer-Invoiced Variable Trade £ (Combo Split)",
    cast(
        "LW CY Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "LW CY Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "LW CY Non-Customer Invoiced Fixed Trade Spend (Combo)" as float
    ) as "LW CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
    cast(
        "LW CY Total Customer-Invoiced Trade (Combo)" as float
    ) as "LW CY Total Customer-Invoiced Trade (Combo)",
    cast(
        "LW CY Total Variable Trade £ (Combo)" as float
    ) as "LW CY Total Variable Trade £ (Combo)",
    cast(
        "LW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "LW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast("LW Forecast PCOS £ (Combo) Act" as float) as "LW Forecast PCOS £ (Combo) Act",
    cast("LW Forecast PCOS £ (Combo) Std" as float) as "LW Forecast PCOS £ (Combo) Std",
    cast(
        "LW Forecast Permanent Discounts £ (Combo)" as float
    ) as "LW Forecast Permanent Discounts £ (Combo)",
    cast(
        "LW (Budget) Forecast Gross Margin £ Act" as float
    ) as "LW (Budget) Forecast Gross Margin £ Act",
    cast(
        "LW (Budget) Forecast Gross Margin £ Std" as float
    ) as "LW (Budget) Forecast Gross Margin £ Std",
    cast(
        "LW (Budget) Forecast Total PCOS Amt £ Std PCOS" as float
    ) as "LW (Budget) Forecast Total PCOS Amt £ Std PCOS",
    cast("LW Budget Fixed Trade Spend" as float) as "LW Budget Fixed Trade Spend",
    cast("LW Budget PCOS Spend Act" as float) as "LW Budget PCOS Spend Act",
    cast("LW Budget PCOS Spend Std" as float) as "LW Budget PCOS Spend Std",
    cast(
        "LW Customer-Invoiced Variable Trade % vs Budget (Split)" as float
    ) as "LW Customer-Invoiced Variable Trade % vs Budget (Split)",
    cast(
        "LW Customer-Invoiced Variable Trade % vs Forecast (Split)" as float
    ) as "LW Customer-Invoiced Variable Trade % vs Forecast (Split)",
    cast(
        "LW Customer-Invoiced Variable Trade % vs LY (Split)" as float
    ) as "LW Customer-Invoiced Variable Trade % vs LY (Split)",
    cast(
        "LW Customer-Invoiced Variable Trade Var to Budget (Split)" as float
    ) as "LW Customer-Invoiced Variable Trade Var to Budget (Split)",
    cast(
        "LW Customer-Invoiced Variable Trade Var to Forecast (Split)" as float
    ) as "LW Customer-Invoiced Variable Trade Var to Forecast (Split)",
    cast(
        "LW Customer-Invoiced Variable Trade Var to LY (Split)" as float
    ) as "LW Customer-Invoiced Variable Trade Var to LY (Split)",
    cast("LW CY NRR £ (Combo)" as float) as "LW CY NRR £ (Combo)",
    cast("LW Cy Variable Trade Spend" as float) as "LW Cy Variable Trade Spend",
    cast(
        "LW Forecast 1 Permanent Discounts £ (Combo)" as float
    ) as "LW Forecast 1 Permanent Discounts £ (Combo)",
    cast(
        "LW Forecast Added Value Pack (Combo)" as float
    ) as "LW Forecast Added Value Pack (Combo)",
    cast(
        "LW Forecast Fixed Trade £ (Combo)" as float
    ) as "LW Forecast Fixed Trade £ (Combo)",
    cast(
        "LW Forecast Fixed Trade Customer-Invoiced" as float
    ) as "LW Forecast Fixed Trade Customer-Invoiced",
    cast("LW Forecast Fixed Trade Spend" as float) as "LW Forecast Fixed Trade Spend",
    cast("LW Forecast GCAT £ (Combo) Act" as float) as "LW Forecast GCAT £ (Combo) Act",
    cast("LW Forecast GCAT £ (Combo) Std" as float) as "LW Forecast GCAT £ (Combo) Std",
    cast(
        "LW Forecast Gross Amount (Combo)" as float
    ) as "LW Forecast Gross Amount (Combo)",
    cast(
        "LW Forecast Net Sales £ (Combo)" as float
    ) as "LW Forecast Net Sales £ (Combo)",
    cast(
        "LW Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "LW Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "LW Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "LW Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "LW Forecast Variable Trade £ (Combo) (1)" as float
    ) as "LW Forecast Variable Trade £ (Combo) (1)",
    cast(
        "LW Forecast Variable Trade Spend" as float
    ) as "LW Forecast Variable Trade Spend",
    cast("Ly Net Sales (Combo)" as number(38, 10)) as "Ly Net Sales (Combo)",
    cast("LW LY NRR £ (Combo)" as float) as "LW LY NRR £ (Combo)",
    cast(
        "LW LY Total Customer-Invoiced Trade (Combo)" as float
    ) as "LW LY Total Customer-Invoiced Trade (Combo)",
    cast(
        "LW Abs Var Forecast Cust-Inv Trade" as float
    ) as "LW Abs Var Forecast Cust-Inv Trade",
    cast("LW Abs Var Forecast GCAT Std" as float) as "LW Abs Var Forecast GCAT Std",
    cast("LW Abs Var Forecast NRR" as float) as "LW Abs Var Forecast NRR",
    cast("LW Abs Var Forecast GCAT Act" as float) as "LW Abs Var Forecast GCAT Act",
    cast("LW CY GCAT £ (Combo) Act" as float) as "LW CY GCAT £ (Combo) Act",
    cast("LW CY GCAT £ (Combo) Std" as float) as "LW CY GCAT £ (Combo) Std",
    cast("LW Forecast NRR £ (Combo)" as float) as "LW Forecast NRR £ (Combo)",
    cast(
        "CW (Budget) Forecast Added Value Pack (Combo)" as float
    ) as "CW (Budget) Forecast Added Value Pack (Combo)",
    cast(
        "CW (Budget) Forecast Gross Amount (Combo)" as float
    ) as "CW (Budget) Forecast Gross Amount (Combo)",
    cast(
        "CW (Budget) Forecast Gross Sales £ (Combo)" as float
    ) as "CW (Budget) Forecast Gross Sales £ (Combo)",
    cast("CW CY Added Value Pack £" as number(38, 10)) as "CW CY Added Value Pack £",
    cast(
        "CW CY Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "CW CY Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "CW (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)" as float
    ) as "CW (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
    cast("CW Cy PCOS Spend (Combo) Std" as float) as "CW Cy PCOS Spend (Combo) Std",
    cast("CW Cy PCOS Spend (Combo) Act" as float) as "CW Cy PCOS Spend (Combo) Act",
    cast(
        "CW (Budget) Forecast Net Sales £ (Combo)" as float
    ) as "CW (Budget) Forecast Net Sales £ (Combo)",
    cast(
        "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "CW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "CW (Budget) Forecast NRR £ (Combo)" as float
    ) as "CW (Budget) Forecast NRR £ (Combo)",
    cast(
        "CW (Budget) Forecast Permanent Discounts £ (Combo)" as float
    ) as "CW (Budget) Forecast Permanent Discounts £ (Combo)",
    cast(
        "CW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "CW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "CW (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "CW (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "CW Budget Variable Trade £ (Combo)" as float
    ) as "CW Budget Variable Trade £ (Combo)",
    cast(
        "CW CY Customer-Invoiced Variable Trade £ (Combo Split)" as float
    ) as "CW CY Customer-Invoiced Variable Trade £ (Combo Split)",
    cast(
        "CW CY Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "CW CY Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast(
        "CW CY Non-Customer Invoiced Fixed Trade Spend (Combo)" as float
    ) as "CW CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
    cast(
        "CW CY Total Customer-Invoiced Trade (Combo)" as float
    ) as "CW CY Total Customer-Invoiced Trade (Combo)",
    cast(
        "CW CY Total Variable Trade £ (Combo)" as float
    ) as "CW CY Total Variable Trade £ (Combo)",
    cast(
        "CW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "CW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast("CW Forecast PCOS £ (Combo) Act" as float) as "CW Forecast PCOS £ (Combo) Act",
    cast("CW Forecast PCOS £ (Combo) Std" as float) as "CW Forecast PCOS £ (Combo) Std",
    cast(
        "CW Forecast Permanent Discounts £ (Combo)" as float
    ) as "CW Forecast Permanent Discounts £ (Combo)",
    cast(
        "CW (Budget) Forecast Gross Margin £ Act" as float
    ) as "CW (Budget) Forecast Gross Margin £ Act",
    cast(
        "CW (Budget) Forecast Gross Margin £ Std" as float
    ) as "CW (Budget) Forecast Gross Margin £ Std",
    cast(
        "CW (Budget) Forecast Total PCOS Amt £ Std PCOS" as float
    ) as "CW (Budget) Forecast Total PCOS Amt £ Std PCOS",
    cast("CW Budget Fixed Trade Spend" as float) as "CW Budget Fixed Trade Spend",
    cast("CW Budget PCOS Spend Act" as float) as "CW Budget PCOS Spend Act",
    cast("CW Budget PCOS Spend Std" as float) as "CW Budget PCOS Spend Std",
    cast(
        "CW Customer-Invoiced Variable Trade % vs Budget (Split)" as float
    ) as "CW Customer-Invoiced Variable Trade % vs Budget (Split)",
    cast(
        "CW Customer-Invoiced Variable Trade % vs Forecast (Split)" as float
    ) as "CW Customer-Invoiced Variable Trade % vs Forecast (Split)",
    cast(
        "CW Customer-Invoiced Variable Trade % vs LY (Split)" as float
    ) as "CW Customer-Invoiced Variable Trade % vs LY (Split)",
    cast(
        "CW Customer-Invoiced Variable Trade Var to Budget (Split)" as float
    ) as "CW Customer-Invoiced Variable Trade Var to Budget (Split)",
    cast(
        "CW Customer-Invoiced Variable Trade Var to Forecast (Split)" as float
    ) as "CW Customer-Invoiced Variable Trade Var to Forecast (Split)",
    cast(
        "CW Customer-Invoiced Variable Trade Var to LY (Split)" as float
    ) as "CW Customer-Invoiced Variable Trade Var to LY (Split)",
    cast("CW CY NRR £ (Combo)" as float) as "CW CY NRR £ (Combo)",
    cast("CW Cy Variable Trade Spend" as float) as "CW Cy Variable Trade Spend",
    cast(
        "CW Forecast 1 Permanent Discounts £ (Combo)" as float
    ) as "CW Forecast 1 Permanent Discounts £ (Combo)",
    cast(
        "CW Forecast Added Value Pack (Combo)" as float
    ) as "CW Forecast Added Value Pack (Combo)",
    cast(
        "CW Forecast Fixed Trade £ (Combo)" as float
    ) as "CW Forecast Fixed Trade £ (Combo)",
    cast(
        "CW Forecast Fixed Trade Customer-Invoiced" as float
    ) as "CW Forecast Fixed Trade Customer-Invoiced",
    cast("CW Forecast Fixed Trade Spend" as float) as "CW Forecast Fixed Trade Spend",
    cast("CW Forecast GCAT £ (Combo) Act" as float) as "CW Forecast GCAT £ (Combo) Act",
    cast("CW Forecast GCAT £ (Combo) Std" as float) as "CW Forecast GCAT £ (Combo) Std",
    cast(
        "CW Forecast Gross Amount (Combo)" as float
    ) as "CW Forecast Gross Amount (Combo)",
    cast(
        "CW Forecast Net Sales £ (Combo)" as float
    ) as "CW Forecast Net Sales £ (Combo)",
    cast(
        "CW Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "CW Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "CW Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "CW Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "CW Forecast Variable Trade £ (Combo) (1)" as float
    ) as "CW Forecast Variable Trade £ (Combo) (1)",
    cast(
        "CW Forecast Variable Trade Spend" as float
    ) as "CW Forecast Variable Trade Spend",
    cast("CW LY NRR £ (Combo)" as float) as "CW LY NRR £ (Combo)",
    cast(
        "CW LY Total Customer-Invoiced Trade (Combo)" as float
    ) as "CW LY Total Customer-Invoiced Trade (Combo)",
    cast(
        "CW Abs Var Forecast Cust-Inv Trade" as float
    ) as "CW Abs Var Forecast Cust-Inv Trade",
    cast("CW Abs Var Forecast GCAT Std" as float) as "CW Abs Var Forecast GCAT Std",
    cast("CW Abs Var Forecast NRR" as float) as "CW Abs Var Forecast NRR",
    cast("CW Abs Var Forecast GCAT Act" as float) as "CW Abs Var Forecast GCAT Act",
    cast("CW CY GCAT £ (Combo) Act" as float) as "CW CY GCAT £ (Combo) Act",
    cast("CW CY GCAT £ (Combo) Std" as float) as "CW CY GCAT £ (Combo) Std",
    cast("CW Forecast NRR £ (Combo)" as float) as "CW Forecast NRR £ (Combo)",
    cast(
        "CW Forecast Non-Customer Invoiced Fixed Trade £ (LIVE)" as float
    ) as "CW Forecast Non-Customer Invoiced Fixed Trade £ (LIVE)",
    cast("CW Forecast PCOS £ (LIVE) Act" as float) as "CW Forecast PCOS £ (LIVE) Act",
    cast("CW Forecast PCOS £ (LIVE) Std" as float) as "CW Forecast PCOS £ (LIVE) Std",
    cast("Cy PCOS Spend (Combo) Act" as float) as "Cy PCOS Spend (Combo) Act",
    cast("Cy PCOS Spend (Combo) Std" as float) as "Cy PCOS Spend (Combo) Std",
    cast(
        "(Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)" as float
    ) as "(Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
    cast(
        "(Budget) Forecast Fixed Trade Customer-Invoiced (Split)" as float
    ) as "(Budget) Forecast Fixed Trade Customer-Invoiced (Split)",
    cast(
        "(Budget) Forecast Net Sales £ (Combo)" as float
    ) as "(Budget) Forecast Net Sales £ (Combo)",
    cast(
        "(Budget) Forecast Permanent Discounts £ (Combo)" as float
    ) as "(Budget) Forecast Permanent Discounts £ (Combo)",
    cast(
        "(Budget) Forecast Total Customer-Invoiced Trade £ (Combo)" as float
    ) as "(Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
    cast(
        "(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)" as float
    ) as "(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
    cast(
        "(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £" as float
    ) as "(Budget) Forecast Total Non-Customer Invoiced Fixed Trade £",
    cast(
        "(Budget) Forecast Total PCOS Amt £ Act PCOS" as float
    ) as "(Budget) Forecast Total PCOS Amt £ Act PCOS",
    cast(
        "CY Customer-Invoiced Variable Trade £ (Combo Split)" as float
    ) as "CY Customer-Invoiced Variable Trade £ (Combo Split)",
    cast("CY GCAT £ Act" as number(38, 10)) as "CY GCAT £ Act",
    cast("CY GCAT £ Std" as number(38, 10)) as "CY GCAT £ Std",
    cast("CY Net Sales £" as number(38, 10)) as "CY Net Sales £",
    cast(
        "CY Non-Customer Invoiced Fixed Trade Spend (Combo)" as float
    ) as "CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
    cast(
        "Forecast Non-Customer Invoiced Fixed Trade £ (Combo)" as float
    ) as "Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    cast("Forecast PCOS £ (Combo) Act" as float) as "Forecast PCOS £ (Combo) Act",
    cast("Forecast PCOS £ (Combo) Std" as float) as "Forecast PCOS £ (Combo) Std",
    cast(
        "Forecast Permanent Discounts £ (Combo)" as float
    ) as "Forecast Permanent Discounts £ (Combo)",
    cast("Forecast 1 AVP £ (Combo)" as float) as "Forecast 1 AVP £ (Combo)",
    cast(
        "Forecast 1 Gross Sales £ (Combo)" as float
    ) as "Forecast 1 Gross Sales £ (Combo)",
    cast(
        "Forecast 1 Net Sales £ (Combo) " as float
    ) as "Forecast 1 Net Sales £ (Combo) ",
    cast(
        "Forecast Fixed Trade Customer-Invoiced (Split)" as float
    ) as "Forecast Fixed Trade Customer-Invoiced (Split)",
    cast("Forecast Gross Amount (Combo)" as float) as "Forecast Gross Amount (Combo)",
    cast("Budget GCAT % vs Budget Act" as float) as "Budget GCAT % vs Budget Act",
    cast("LW GCAT % vs Budget Act" as float) as "LW GCAT % vs Budget Act",
    cast("CW GCAT % vs Budget Act" as float) as "CW GCAT % vs Budget Act",
    cast("LE GCAT % vs Budget Act" as float) as "LE GCAT % vs Budget Act",
    cast("Budget GCAT % vs Budget Std" as float) as "Budget GCAT % vs Budget Std",
    cast("LE GCAT % vs Budget Std" as float) as "LE GCAT % vs Budget Std",
    cast("CW GCAT % vs Budget Std" as float) as "CW GCAT % vs Budget Std",
    cast("LW GCAT % vs Budget Std" as float) as "LW GCAT % vs Budget Std",
    cast("Budget GCAT % vs Forecast Act" as float) as "Budget GCAT % vs Forecast Act",
    cast("LW GCAT % vs Forecast Act" as float) as "LW GCAT % vs Forecast Act",
    cast("CW GCAT % vs Forecast Act" as float) as "CW GCAT % vs Forecast Act",
    cast("LE GCAT % vs Forecast Act" as float) as "LE GCAT % vs Forecast Act",
    cast("Budget GCAT % vs Forecast Std" as float) as "Budget GCAT % vs Forecast Std",
    cast("LW GCAT % vs Forecast Std" as float) as "LW GCAT % vs Forecast Std",
    cast("CW GCAT % vs Forecast Std" as float) as "CW GCAT % vs Forecast Std",
    cast("LE GCAT % vs Forecast Std" as float) as "LE GCAT % vs Forecast Std",
    cast("LY Net Sales £" as number(38, 10)) as "LY Net Sales £",
    cast("LY NRR £" as number(38, 10)) as "LY NRR £",
    cast("LY GCAT £ Act" as number(38, 10)) as "LY GCAT £ Act",
    cast("LY GCAT £ Std" as number(38, 10)) as "LY GCAT £ Std",
    cast("LY Total Fixed Trade £" as number(38, 10)) as "LY Total Fixed Trade £",
    cast("LY Total PCOS Amt £" as number(38, 10)) as "LY Total PCOS Amt £",
    cast("Budget GCAT % YoY Std" as float) as "Budget GCAT % YoY Std",
    cast("LW GCAT % YoY Std" as float) as "LW GCAT % YoY Std",
    cast("LE GCAT % YoY Std" as float) as "LE GCAT % YoY Std",
    cast("CW GCAT % YoY Std" as float) as "CW GCAT % YoY Std",
    cast("Budget GCAT % YoY Act" as float) as "Budget GCAT % YoY Act",
    cast("LE GCAT % YoY Act" as float) as "LE GCAT % YoY Act",
    cast("CW GCAT % YoY Act" as float) as "CW GCAT % YoY Act",
    cast("LW GCAT % YoY Act" as float) as "LW GCAT % YoY Act",
    cast("Budget GCAT Var to Budget Std" as float) as "Budget GCAT Var to Budget Std",
    cast("LE GCAT Var to Budget Std" as float) as "LE GCAT Var to Budget Std",
    cast("CW GCAT Var to Budget Std" as float) as "CW GCAT Var to Budget Std",
    cast("LW GCAT Var to Budget Std" as float) as "LW GCAT Var to Budget Std",
    cast("Budget GCAT Var to Budget Act" as float) as "Budget GCAT Var to Budget Act",
    cast("LW GCAT Var to Budget Act" as float) as "LW GCAT Var to Budget Act",
    cast("CW GCAT Var to Budget Act" as float) as "CW GCAT Var to Budget Act",
    cast("LE GCAT Var to Budget Act" as float) as "LE GCAT Var to Budget Act",
    cast(
        "Budget GCAT Var to Forecast Std" as float
    ) as "Budget GCAT Var to Forecast Std",
    cast("CW GCAT Var to Forecast Std" as float) as "CW GCAT Var to Forecast Std",
    cast("LW GCAT Var to Forecast Std" as float) as "LW GCAT Var to Forecast Std",
    cast("LE GCAT Var to Forecast Std" as float) as "LE GCAT Var to Forecast Std",
    cast(
        "Budget GCAT Var to Forecast Act" as float
    ) as "Budget GCAT Var to Forecast Act",
    cast("LE GCAT Var to Forecast Act" as float) as "LE GCAT Var to Forecast Act",
    cast("LW GCAT Var to Forecast Act" as float) as "LW GCAT Var to Forecast Act",
    cast("CW GCAT Var to Forecast Act" as float) as "CW GCAT Var to Forecast Act",
    cast("Budget GCAT Var to LY Std" as float) as "Budget GCAT Var to LY Std",
    cast("Budget GCAT Var to LY Act" as float) as "Budget GCAT Var to LY Act",
    cast("LE GCAT Var to LY Std" as float) as "LE GCAT Var to LY Std",
    cast("LE GCAT Var to LY Act" as float) as "LE GCAT Var to LY Act",
    cast("LW GCAT Var to LY Std" as float) as "LW GCAT Var to LY Std",
    cast("LW GCAT Var to LY Act" as float) as "LW GCAT Var to LY Act",
    cast("CW GCAT Var to LY Std" as float) as "CW GCAT Var to LY Std",
    cast("CW GCAT Var to LY Act" as float) as "CW GCAT Var to LY Act",
    cast(
        "LY Customer-Invoiced Variable Trade £ (Combo Split)" as number(38, 10)
    ) as "LY Customer-Invoiced Variable Trade £ (Combo Split)",
    cast("Ly Fixed Trade Spend" as number(38, 10)) as "Ly Fixed Trade Spend",
    cast(
        "LY Non-Customer Invoiced Fixed Trade Spend" as number(38, 10)
    ) as "LY Non-Customer Invoiced Fixed Trade Spend",
    cast("Ly PCOS Spend Std" as number(38, 10)) as "Ly PCOS Spend Std",
    cast("Ly PCOS Spend Act" as number(38, 10)) as "Ly PCOS Spend Act",
    cast(
        "LY Total Customer-Invoiced Trade Spend" as number(38, 10)
    ) as "LY Total Customer-Invoiced Trade Spend",
    cast("Ly Variable Trade Spend" as number(38, 10)) as "Ly Variable Trade Spend",
    cast("Budget Net Sales % vs Budget" as float) as "Budget Net Sales % vs Budget",
    cast("LW Net Sales % vs Budget" as float) as "LW Net Sales % vs Budget",
    cast("CW Net Sales % vs Budget" as float) as "CW Net Sales % vs Budget",
    cast("LE Net Sales % vs Budget" as float) as "LE Net Sales % vs Budget",
    cast("Budget Net Sales % vs Forecast" as float) as "Budget Net Sales % vs Forecast",
    cast("LE Net Sales % vs Forecast" as float) as "LE Net Sales % vs Forecast",
    cast("CW Net Sales % vs Forecast" as float) as "CW Net Sales % vs Forecast",
    cast("LW Net Sales % vs Forecast" as float) as "LW Net Sales % vs Forecast",
    cast("Net Sales % YoY" as float) as "Net Sales % YoY",
    cast("Budget Net Sales Var to Budget" as float) as "Budget Net Sales Var to Budget",
    cast("LW Net Sales Var to Budget" as float) as "LW Net Sales Var to Budget",
    cast("CW Net Sales Var to Budget" as float) as "CW Net Sales Var to Budget",
    cast("LE Net Sales Var to Budget" as float) as "LE Net Sales Var to Budget",
    cast(
        "Budget Net Sales Var to Forecast" as float
    ) as "Budget Net Sales Var to Forecast",
    cast("CW Net Sales Var to Forecast" as float) as "CW Net Sales Var to Forecast",
    cast("LW Net Sales Var to Forecast" as float) as "LW Net Sales Var to Forecast",
    cast("LE Net Sales Var to Forecast" as float) as "LE Net Sales Var to Forecast",
    cast("Net Sales Var to LY" as float) as "Net Sales Var to LY",
    cast(
        "Budget Non-Customer Fixed Trade Var to Budget" as float
    ) as "Budget Non-Customer Fixed Trade Var to Budget",
    cast(
        "LW Non-Customer Fixed Trade Var to Budget" as float
    ) as "LW Non-Customer Fixed Trade Var to Budget",
    cast(
        "LE Non-Customer Fixed Trade Var to Budget" as float
    ) as "LE Non-Customer Fixed Trade Var to Budget",
    cast(
        "CW Non-Customer Fixed Trade Var to Budget" as float
    ) as "CW Non-Customer Fixed Trade Var to Budget",
    cast(
        "Budget Non-Customer Inv Trade % vs Budget" as float
    ) as "Budget Non-Customer Inv Trade % vs Budget",
    cast(
        "LW Non-Customer Inv Trade % vs Budget" as float
    ) as "LW Non-Customer Inv Trade % vs Budget",
    cast(
        "LE Non-Customer Inv Trade % vs Budget" as float
    ) as "LE Non-Customer Inv Trade % vs Budget",
    cast(
        "CW Non-Customer Inv Trade % vs Budget" as float
    ) as "CW Non-Customer Inv Trade % vs Budget",
    cast(
        "Budget Non-Customer Inv Trade % vs Forecast" as float
    ) as "Budget Non-Customer Inv Trade % vs Forecast",
    cast(
        "LW Non-Customer Inv Trade % vs Forecast" as float
    ) as "LW Non-Customer Inv Trade % vs Forecast",
    cast(
        "LE Non-Customer Inv Trade % vs Forecast" as float
    ) as "LE Non-Customer Inv Trade % vs Forecast",
    cast(
        "CW Non-Customer Inv Trade % vs Forecast" as float
    ) as "CW Non-Customer Inv Trade % vs Forecast",
    cast(
        "Budget Non-Customer Inv Trade % YoY" as float
    ) as "Budget Non-Customer Inv Trade % YoY",
    cast(
        "LE Non-Customer Inv Trade % YoY" as float
    ) as "LE Non-Customer Inv Trade % YoY",
    cast(
        "LW Non-Customer Inv Trade % YoY" as float
    ) as "LW Non-Customer Inv Trade % YoY",
    cast(
        "CW Non-Customer Inv Trade % YoY" as float
    ) as "CW Non-Customer Inv Trade % YoY",
    cast(
        "Budget Non-Customer Inv Trade Var to Forecast" as float
    ) as "Budget Non-Customer Inv Trade Var to Forecast",
    cast(
        "LW Non-Customer Inv Trade Var to Forecast" as float
    ) as "LW Non-Customer Inv Trade Var to Forecast",
    cast(
        "CW Non-Customer Inv Trade Var to Forecast" as float
    ) as "CW Non-Customer Inv Trade Var to Forecast",
    cast(
        "LE Non-Customer Inv Trade Var to Forecast" as float
    ) as "LE Non-Customer Inv Trade Var to Forecast",
    cast(
        "Budget Non-Customer Inv Trade Var to LY" as float
    ) as "Budget Non-Customer Inv Trade Var to LY",
    cast(
        "LW Non-Customer Inv Trade Var to LY" as float
    ) as "LW Non-Customer Inv Trade Var to LY",
    cast(
        "CW Non-Customer Inv Trade Var to LY" as float
    ) as "CW Non-Customer Inv Trade Var to LY",
    cast(
        "LE Non-Customer Inv Trade Var to LY" as float
    ) as "LE Non-Customer Inv Trade Var to LY",
    cast("Budget NRR % Var to Budget" as float) as "Budget NRR % Var to Budget",
    cast("CW NRR % Var to Budget" as float) as "CW NRR % Var to Budget",
    cast("LE NRR % Var to Budget" as float) as "LE NRR % Var to Budget",
    cast("LW NRR % Var to Budget" as float) as "LW NRR % Var to Budget",
    cast("Budget NRR % Var to Forecast" as float) as "Budget NRR % Var to Forecast",
    cast("LW NRR % Var to Forecast" as float) as "LW NRR % Var to Forecast",
    cast("CW NRR % Var to Forecast" as float) as "CW NRR % Var to Forecast",
    cast("LE NRR % Var to Forecast" as float) as "LE NRR % Var to Forecast",
    cast("Budget NRR % YoY" as float) as "Budget NRR % YoY",
    cast("LW NRR % YoY" as float) as "LW NRR % YoY",
    cast("CW NRR % YoY" as float) as "CW NRR % YoY",
    cast("LE NRR % YoY" as float) as "LE NRR % YoY",
    cast("Budget NRR Var to Budget" as float) as "Budget NRR Var to Budget",
    cast("CW NRR Var to Budget" as float) as "CW NRR Var to Budget",
    cast("LE NRR Var to Budget" as float) as "LE NRR Var to Budget",
    cast("LW NRR Var to Budget" as float) as "LW NRR Var to Budget",
    cast("Budget NRR Var to Forecast" as float) as "Budget NRR Var to Forecast",
    cast("LW NRR Var to Forecast" as float) as "LW NRR Var to Forecast",
    cast("CW NRR Var to Forecast" as float) as "CW NRR Var to Forecast",
    cast("LE NRR Var to Forecast" as float) as "LE NRR Var to Forecast",
    cast("Budget NRR YoY" as float) as "Budget NRR YoY",
    cast("LE NRR YoY" as float) as "LE NRR YoY",
    cast("CW NRR YoY" as float) as "CW NRR YoY",
    cast("LW NRR YoY" as float) as "LW NRR YoY",
    cast("Budget PCOS % vs Budget Std" as float) as "Budget PCOS % vs Budget Std",
    cast("LE PCOS % vs Budget Std" as float) as "LE PCOS % vs Budget Std",
    cast("CW PCOS % vs Budget Std" as float) as "CW PCOS % vs Budget Std",
    cast("LW PCOS % vs Budget Std" as float) as "LW PCOS % vs Budget Std",
    cast("Budget PCOS % vs Budget Act" as float) as "Budget PCOS % vs Budget Act",
    cast("LE PCOS % vs Budget Act" as float) as "LE PCOS % vs Budget Act",
    cast("CW PCOS % vs Budget Act" as float) as "CW PCOS % vs Budget Act",
    cast("LW PCOS % vs Budget Act" as float) as "LW PCOS % vs Budget Act",
    cast("Budget PCOS % vs Forecast Std" as float) as "Budget PCOS % vs Forecast Std",
    cast("LE PCOS % vs Forecast Std" as float) as "LE PCOS % vs Forecast Std",
    cast("CW PCOS % vs Forecast Std" as float) as "CW PCOS % vs Forecast Std",
    cast("LW PCOS % vs Forecast Std" as float) as "LW PCOS % vs Forecast Std",
    cast("Budget PCOS % vs Forecast Act" as float) as "Budget PCOS % vs Forecast Act",
    cast("LE PCOS % vs Forecast Act" as float) as "LE PCOS % vs Forecast Act",
    cast("CW PCOS % vs Forecast Act" as float) as "CW PCOS % vs Forecast Act",
    cast("LW PCOS % vs Forecast Act" as float) as "LW PCOS % vs Forecast Act",
    cast("Budget PCOS % YoY Std" as float) as "Budget PCOS % YoY Std",
    cast("LW PCOS % YoY Std" as float) as "LW PCOS % YoY Std",
    cast("CW PCOS % YoY Std" as float) as "CW PCOS % YoY Std",
    cast("LE PCOS % YoY Std" as float) as "LE PCOS % YoY Std",
    cast("Budget PCOS % YoY Act" as float) as "Budget PCOS % YoY Act",
    cast("LW PCOS % YoY Act" as float) as "LW PCOS % YoY Act",
    cast("CW PCOS % YoY Act" as float) as "CW PCOS % YoY Act",
    cast("LE PCOS % YoY Act" as float) as "LE PCOS % YoY Act",
    cast("Budget PCOS Var to Budget Std" as float) as "Budget PCOS Var to Budget Std",
    cast("LE PCOS Var to Budget Std" as float) as "LE PCOS Var to Budget Std",
    cast("LW PCOS Var to Budget Std" as float) as "LW PCOS Var to Budget Std",
    cast("CW PCOS Var to Budget Std" as float) as "CW PCOS Var to Budget Std",
    cast("Budget PCOS Var to Budget Act" as float) as "Budget PCOS Var to Budget Act",
    cast("LE PCOS Var to Budget Act" as float) as "LE PCOS Var to Budget Act",
    cast("LW PCOS Var to Budget Act" as float) as "LW PCOS Var to Budget Act",
    cast("CW PCOS Var to Budget Act" as float) as "CW PCOS Var to Budget Act",
    cast(
        "Budget PCOS Var to Forecast Std" as float
    ) as "Budget PCOS Var to Forecast Std",
    cast("LE PCOS Var to Forecast Std" as float) as "LE PCOS Var to Forecast Std",
    cast("LW PCOS Var to Forecast Std" as float) as "LW PCOS Var to Forecast Std",
    cast("CW PCOS Var to Forecast Std" as float) as "CW PCOS Var to Forecast Std",
    cast(
        "Budget PCOS Var to Forecast Act" as float
    ) as "Budget PCOS Var to Forecast Act",
    cast("LE PCOS Var to Forecast Act" as float) as "LE PCOS Var to Forecast Act",
    cast("LW PCOS Var to Forecast Act" as float) as "LW PCOS Var to Forecast Act",
    cast("CW PCOS Var to Forecast Act" as float) as "CW PCOS Var to Forecast Act",
    cast("Budget PCOS Var to LY Std" as float) as "Budget PCOS Var to LY Std",
    cast("LE PCOS Var to LY Std" as float) as "LE PCOS Var to LY Std",
    cast("LW PCOS Var to LY Std" as float) as "LW PCOS Var to LY Std",
    cast("CW PCOS Var to LY Std" as float) as "CW PCOS Var to LY Std",
    cast("Budget PCOS Var to LY Act" as float) as "Budget PCOS Var to LY Act",
    cast("LE PCOS Var to LY Act" as float) as "LE PCOS Var to LY Act",
    cast("LW PCOS Var to LY Act" as float) as "LW PCOS Var to LY Act",
    cast("CW PCOS Var to LY Act" as float) as "CW PCOS Var to LY Act",
    cast(
        "Budget Selected Budget GCAT £ Std" as float
    ) as "Budget Selected Budget GCAT £ Std",
    cast("LE Selected Budget GCAT £ Std" as float) as "LE Selected Budget GCAT £ Std",
    cast("LW Selected Budget GCAT £ Std" as float) as "LW Selected Budget GCAT £ Std",
    cast("CW Selected Budget GCAT £ Std" as float) as "CW Selected Budget GCAT £ Std",
    cast(
        "Budget Selected Budget GCAT £ Act" as float
    ) as "Budget Selected Budget GCAT £ Act",
    cast("LE Selected Budget GCAT £ Act" as float) as "LE Selected Budget GCAT £ Act",
    cast("LW Selected Budget GCAT £ Act" as float) as "LW Selected Budget GCAT £ Act",
    cast("CW Selected Budget GCAT £ Act" as float) as "CW Selected Budget GCAT £ Act",
    cast(
        "Budget Total Customer-Invoiced % vs Budget" as float
    ) as "Budget Total Customer-Invoiced % vs Budget",
    cast(
        "LE Total Customer-Invoiced % vs Budget" as float
    ) as "LE Total Customer-Invoiced % vs Budget",
    cast(
        "LW Total Customer-Invoiced % vs Budget" as float
    ) as "LW Total Customer-Invoiced % vs Budget",
    cast(
        "CW Total Customer-Invoiced % vs Budget" as float
    ) as "CW Total Customer-Invoiced % vs Budget",
    cast(
        "Budget Total Customer-Invoiced % vs Forecast" as float
    ) as "Budget Total Customer-Invoiced % vs Forecast",
    cast(
        "LW Total Customer-Invoiced % vs Forecast" as float
    ) as "LW Total Customer-Invoiced % vs Forecast",
    cast(
        "CW Total Customer-Invoiced % vs Forecast" as float
    ) as "CW Total Customer-Invoiced % vs Forecast",
    cast(
        "LE Total Customer-Invoiced % vs Forecast" as float
    ) as "LE Total Customer-Invoiced % vs Forecast",
    cast(
        "Budget Total Customer-Invoiced % YoY" as float
    ) as "Budget Total Customer-Invoiced % YoY",
    cast(
        "LW Total Customer-Invoiced % YoY" as float
    ) as "LW Total Customer-Invoiced % YoY",
    cast(
        "CW Total Customer-Invoiced % YoY" as float
    ) as "CW Total Customer-Invoiced % YoY",
    cast(
        "LE Total Customer-Invoiced % YoY" as float
    ) as "LE Total Customer-Invoiced % YoY",
    cast(
        "Budget Total Customer-Invoiced Var to Budget" as float
    ) as "Budget Total Customer-Invoiced Var to Budget",
    cast(
        "CW Total Customer-Invoiced Var to Budget" as float
    ) as "CW Total Customer-Invoiced Var to Budget",
    cast(
        "LE Total Customer-Invoiced Var to Budget" as float
    ) as "LE Total Customer-Invoiced Var to Budget",
    cast(
        "LW Total Customer-Invoiced Var to Budget" as float
    ) as "LW Total Customer-Invoiced Var to Budget",
    cast(
        "Budget Total Customer-Invoiced Var to Forecast" as float
    ) as "Budget Total Customer-Invoiced Var to Forecast",
    cast(
        "LW Total Customer-Invoiced Var to Forecast" as float
    ) as "LW Total Customer-Invoiced Var to Forecast",
    cast(
        "LE Total Customer-Invoiced Var to Forecast" as float
    ) as "LE Total Customer-Invoiced Var to Forecast",
    cast(
        "CW Total Customer-Invoiced Var to Forecast" as float
    ) as "CW Total Customer-Invoiced Var to Forecast",
    cast(
        "Budget Total Customer-Invoiced Var to LY" as float
    ) as "Budget Total Customer-Invoiced Var to LY",
    cast(
        "CW Total Customer-Invoiced Var to LY" as float
    ) as "CW Total Customer-Invoiced Var to LY",
    cast(
        "LE Total Customer-Invoiced Var to LY" as float
    ) as "LE Total Customer-Invoiced Var to LY",
    cast(
        "LW Total Customer-Invoiced Var to LY" as float
    ) as "LW Total Customer-Invoiced Var to LY",
    cast(
        "Budget Variable Trade % vs Budget" as float
    ) as "Budget Variable Trade % vs Budget",
    cast("CW Variable Trade % vs Budget" as float) as "CW Variable Trade % vs Budget",
    cast("LE Variable Trade % vs Budget" as float) as "LE Variable Trade % vs Budget",
    cast("LW Variable Trade % vs Budget" as float) as "LW Variable Trade % vs Budget",
    cast(
        "Budget Variable Trade % vs Forecast" as float
    ) as "Budget Variable Trade % vs Forecast",
    cast(
        "LW Variable Trade % vs Forecast" as float
    ) as "LW Variable Trade % vs Forecast",
    cast(
        "LE Variable Trade % vs Forecast" as float
    ) as "LE Variable Trade % vs Forecast",
    cast(
        "CW Variable Trade % vs Forecast" as float
    ) as "CW Variable Trade % vs Forecast",
    cast("Budget Variable Trade % YoY" as float) as "Budget Variable Trade % YoY",
    cast("CW Variable Trade % YoY" as float) as "CW Variable Trade % YoY",
    cast("LE Variable Trade % YoY" as float) as "LE Variable Trade % YoY",
    cast("LW Variable Trade % YoY" as float) as "LW Variable Trade % YoY",
    cast(
        "Budget Variable Trade vs Budget" as float
    ) as "Budget Variable Trade vs Budget",
    cast("CW Variable Trade vs Budget" as float) as "CW Variable Trade vs Budget",
    cast("LW Variable Trade vs Budget" as float) as "LW Variable Trade vs Budget",
    cast("LE Variable Trade vs Budget" as float) as "LE Variable Trade vs Budget",
    cast(
        "Budget Variable Trade vs Forecast" as float
    ) as "Budget Variable Trade vs Forecast",
    cast("LW Variable Trade vs Forecast" as float) as "LW Variable Trade vs Forecast",
    cast("CW Variable Trade vs Forecast" as float) as "CW Variable Trade vs Forecast",
    cast("LE Variable Trade vs Forecast" as float) as "LE Variable Trade vs Forecast",
    cast("Budget Variable Trade vs Ly" as float) as "Budget Variable Trade vs Ly",
    cast("LW Variable Trade vs Ly" as float) as "LW Variable Trade vs Ly",
    cast("CW Variable Trade vs Ly" as float) as "CW Variable Trade vs Ly",
    cast("LE Variable Trade vs Ly" as float) as "LE Variable Trade vs Ly",
    cast("CY NRR £" as float) as "CY NRR £"
from final