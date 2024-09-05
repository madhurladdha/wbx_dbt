{{
    config(
        materialized=env_var("DBT_MAT_VIEW"),
        tags=["sales", "performance", "sales_performance_lite"],
    )
}}

/*  currency_symbol is embedded in field names so that the symbol can be easily changed.
    currency_string is the currency code string used in the incoming field names (source fields) that are to mapped for the output.  this will NOT 
        handle every field, but should handle the actuals (not forecast or budgets)
*/
{% set currency_symbol = "€" %}
{% set currency_string = "base" %}

with
    dim_date_cte as (select * from {{ ref("src_dim_date") }}),
    scen_xref as (select * from {{ ref("src_sls_wtx_budget_scen_xref") }}),
    uber_cte as (select * from {{ ref("fct_wbx_sls_uber") }}),
    uber as (
        select *
        from uber_cte
        where
            frozen_forecast in (
                select distinct frozen_forecast
                from uber_cte
                where
                    right(frozen_forecast, 2) = (
                        select max(right(frozen_forecast, 2)) as frozen_forecast_year
                        from uber_cte
                        where
                            (nvl(document_company, '-')) in ('RFL','IBE', '-')
                            and nvl(source_content_filter, '-')
                            not in ('EPOS', 'TERMS', '-')
                            and frozen_forecast like 'BUDGET%'
                    )
                    and (nvl(document_company, '-')) in ('RFL','IBE','-')
                    and nvl(source_content_filter, '-') not in ('EPOS', 'TERMS', '-')
                    and frozen_forecast like 'BUDGET%'
                group by frozen_forecast
            )
    ),
    item_master_ext as (select * from {{ ref("dim_wbx_item_ext") }}),
    cust_master_ext as (select * from {{ ref("dim_wbx_customer_ext") }}),
    cust_planning as (select * from {{ ref("dim_wbx_cust_planning") }}),
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
            as budget_unique_key,
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
            ) as "Forecast Customer-Invoiced Fixed Trade {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_variable_trade, 0)) as "Forecast Total Variable Trade {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_ext_boughtin_amt, 0)) as "Cy Std Bought-In Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_ext_copack_amt, 0)) as "Cy Std Co-Packing Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_ext_ing_amt, 0)) as "Cy Std Ingredient Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_ext_lbr_amt, 0)) as "Cy Std Labour Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_ext_oth_amt, 0)) as "Cy Std Other Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_ext_pkg_amt, 0)) as "Cy Std Packaging Amt {{currency_symbol}}",
            sum(
                nvl(ub.cy_{{currency_string}}_pcos_boughtin_var, 0)
            ) as "Cy Var to Std Bought-In Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_pcos_copack_var, 0)) as "Cy Var to Std Co-Packing Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_pcos_raw_var, 0)) as "Cy Var to Std Ingredients Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_pcos_labour_var, 0)) as "Cy Var to Std Labour Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_pcos_other_var, 0)) as "Cy Var to Std Other Amt {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_pcos_pack_var, 0)) as "Cy Var to Std Packaging Amt {{currency_symbol}}",
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
            sum(nvl(ub.cy_{{currency_string}}_rpt_grs_amt, 0)) as "Cy Gross Amount {{currency_symbol}}",
            sum(nvl(ub.cy_{{currency_string}}_invoice_grs_amt, 0)) as "Cy Invoice Gross Amount {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_rpt_grs_amt, 0)) as "Ly Gross Amount {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_cat, 0)) as "Cy Category {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_drct_shp, 0)) as "Cy Direct Shopper Marking {{currency_symbol}}",
            sum(
                nvl(ub.cy_gl_{{currency_string}}_permd_csh_disc, 0)
            ) as "Cy Early Settlement Discount {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_permd_edlp, 0)) as "Cy EDLP {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_ag_cst, 0)) as "Cy Field Marketing {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_fxd_pymt, 0)) as "Cy Fixed Annual Pymts {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_permd_rsa_inct, 0)) as "Cy Growth Incentives {{currency_symbol}}",
            sum(
                nvl(ub.cy_gl_{{currency_string}}_trade_indrct_shp, 0)
            ) as "Cy Indirect Shopper Marking {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_oth_drct_pymt, 0)) as "Cy Other Direct Pymts {{currency_symbol}}",
            sum(
                nvl(ub.cy_gl_{{currency_string}}_trade_oth_indrct_pymt, 0)
            ) as "Cy Other Indirect Pymts {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_other, 0)) as "Cy Other Trade {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_promo_fixed, 0)) as "Cy Promo Fixed Funding {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_permd_rng_spt, 0)) as "Cy Range Support Allowance {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_retro, 0)) as "Cy Retro {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_added_value_pack, 0)) as "Forecast Added Value Pack {{currency_symbol}}",
            sum(
                nvl(ub.fcf_ap_early_settlement_disc, 0)
            ) as "Forecast Early Settlement Discount {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_everyday_low_prices, 0)) as "Forecast EDLP {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_gross_selling_value, 0)) as "Forecast Gross Amount {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_growth_incentives, 0)) as "Forecast Growth Incentives {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_net_sales_value, 0)) as "Forecast Net Sales {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_tot_prime_cost_standard, 0)) as "Forecast PCOS (Std) {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_tot_prime_cost_variance, 0)) as "Forecast PCOS (Var) {{currency_symbol}}",
            sum(nvl(ub.fcf_ap_permanent_disc, 0)) as "Forecast Range Support {{currency_symbol}}",
            sum(
                nvl(ub.fcf_ap_range_support_incentives, 0)
            ) as "Forecast RSA Incentives {{currency_symbol}}",
            sum(
                nvl(ub.fcf_ap_total_trade_cust_invoiced, 0)
            ) as "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
            sum(
                nvl(ub.fcf_ap_fixed_trade_non_cust_invoiced, 0)
            ) as "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
            sum(nvl(ub.fcf_tot_vol_ca, 0)) as "Forecast Total Volume Case",
            sum(
                nvl(ub.fcf_tot_vol_ca, 0) * itm_ext.consumer_units
            ) as "Forecast Total Volume Consumer Units",
            sum(nvl(ub.fcf_tot_vol_kg, 0)) as "Forecast Total Volume Kg",
            sum(
                nvl(ub.fcf_tot_vol_ca, 0) * itm_ext.consumer_units_in_trade_units
            ) as "Forecast Total Volume Packet",
            sum(nvl(ub.fcf_tot_vol_ul, 0)) as "Forecast Total Volume Pallet",
            sum(nvl(ub.budget_amount, 0)) as "Budget Gross Amount {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_ext_boughtin_amt, 0)) as "Ly Std Bought-In Amt {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_ext_copack_amt, 0)) as "Ly Std Co-Packing Amt {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_ext_ing_amt, 0)) as "Ly Std Ingredient Amt {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_ext_lbr_amt, 0)) as "Ly Std Labour Amt {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_ext_oth_amt, 0)) as "Ly Std Other Amt {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_ext_pkg_amt, 0)) as "Ly Std Packaging Amt {{currency_symbol}}",
            sum(
                nvl(ub.ly_{{currency_string}}_pcos_boughtin_var, 0)
            ) as "Ly Var to Std Bought-In Amt {{currency_symbol}}",
            sum(
                nvl(ub.ly_{{currency_string}}_pcos_copack_var, 0)
            ) as "Ly Var to Std Co-Packing Amt  {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_pcos_raw_var, 0)) as "Ly Var to Std Ingredients Amt  {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_pcos_labour_var, 0)) as "Ly Var to Std Labour Amt  {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_pcos_other_var, 0)) as "Ly Var to Std Other Amt  {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_pcos_pack_var, 0)) as "Ly Var to Std Packaging Amt  {{currency_symbol}}",
            sum(nvl(ub.ly_{{currency_string}}_invoice_grs_amt, 0)) as "Ly Invoice Gross Amount {{currency_symbol}}",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_avp, 0)) as "Cy AVP Discount {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_avp, 0)) as "Ly AVP Discount {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_cat, 0)) as "Ly Category {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_drct_shp, 0)) as "Ly Direct Shopper Marking {{currency_symbol}}",
            sum(
                nvl(ub.ly_gl_{{currency_string}}_permd_csh_disc, 0)
            ) as "Ly Early Settlement Discount {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_permd_edlp, 0)) as "Ly EDLP {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_ag_cst, 0)) as "Ly Field Marketing {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_fxd_pymt, 0)) as "Ly Fixed Annual Pymts {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_permd_rsa_inct, 0)) as "Ly Growth Incentives {{currency_symbol}}",
            sum(
                nvl(ub.ly_gl_{{currency_string}}_trade_indrct_shp, 0)
            ) as "Ly Indirect Shopper Marking {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_oth_drct_pymt, 0)) as "Ly Other Direct Pymts {{currency_symbol}}",
            sum(
                nvl(ub.ly_gl_{{currency_string}}_trade_oth_indrct_pymt, 0)
            ) as "Ly Other Indirect Pymts {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_other, 0)) as "Ly Other Trade {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_promo_fixed, 0)) as "Ly Promo Fixed Funding {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_permd_rng_spt, 0)) as "Ly Range Support Allowance {{currency_symbol}}",
            sum(nvl(ub.ly_gl_{{currency_string}}_trade_retro, 0)) as "Ly Retro {{currency_symbol}}",
            sum(
                nvl(ub.cy_gl_{{currency_string}}_trade_avp, 0) * -1 + ub.fcf_ap_added_value_pack
            ) as "(Budget) Forecast Added Value Pack (Combo)",
            sum(
                nvl(ub.cy_{{currency_string}}_invoice_grs_amt, 0)
                + nvl(ub.fcf_ap_gross_selling_value, 0)
            ) as "(Budget) Forecast Gross Amount (Combo)",
            sum(
                (
                    nvl(ub.cy_{{currency_string}}_invoice_grs_amt, 0)
                    + nvl(ub.fcf_ap_gross_selling_value, 0)
                )
                + nvl(((ub.cy_gl_{{currency_string}}_trade_avp * -1) + ub.fcf_ap_added_value_pack), 0)
            ) as "(Budget) Forecast Gross Sales {{currency_symbol}} (Combo)",

            sum(nvl(ub.fcf_ap_variable_trade, 0)) as "Budget Variable Trade Spend",
            sum(nvl(ub.cy_gl_{{currency_string}}_trade_avp, 0) * -1) as "CY Added Value Pack {{currency_symbol}}",
            sum(
                ifnull(cy_gl_{{currency_string}}_trade_promo_fixed, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_fxd_pymt, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_drct_shp, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_oth_drct_pymt, 0)
            ) as "CY Customer-Invoiced Fixed Trade {{currency_symbol}}",
            sum(
                ifnull(cy_{{currency_string}}_invoice_grs_amt, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_avp * -1, 0)
            ) as "CY Gross Sales {{currency_symbol}}",
            sum(
                nvl(ub.cy_{{currency_string}}_ext_boughtin_amt, 0)
                + nvl(ub.cy_{{currency_string}}_ext_copack_amt, 0)
                + nvl(ub.cy_{{currency_string}}_ext_ing_amt, 0)
                + nvl(ub.cy_{{currency_string}}_ext_lbr_amt, 0)
                + nvl(ub.cy_{{currency_string}}_ext_oth_amt, 0)
                + nvl(ub.cy_{{currency_string}}_ext_pkg_amt, 0)
            ) as "CY Total Std PCOS Amt {{currency_symbol}}",
            sum(
                nvl(
                    nvl(ub.cy_{{currency_string}}_pcos_boughtin_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_copack_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_raw_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_labour_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_other_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_pack_var, 0),
                    0
                )
            ) as "CY Total Var to Std PCOS Amt {{currency_symbol}}",
            sum(
                ifnull(cy_gl_{{currency_string}}_permd_rng_spt, 0)
                + ifnull(cy_gl_{{currency_string}}_permd_edlp, 0)
                + ifnull(cy_gl_{{currency_string}}_permd_csh_disc, 0)
                + ifnull(cy_gl_{{currency_string}}_permd_rsa_inct, 0)
            ) as "CY Permanent Discounts {{currency_symbol}}",
            sum(
                ifnull(cy_gl_{{currency_string}}_trade_indrct_shp, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_cat, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_oth_indrct_pymt, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_ag_cst, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_other, 0)
            ) as "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
            sum(
                ifnull(cy_gl_{{currency_string}}_trade_retro, 0) + ifnull(cy_gl_{{currency_string}}_trade_avp, 0)
            ) as "CY Total Variable Trade {{currency_symbol}}",
            sum(
                ifnull(cy_{{currency_string}}_invoice_grs_amt + fcf_ap_gross_selling_value, 0)
            ) as "Forecast 1 Gross Amount {{currency_symbol}} (Combo)",
            sum(
                ub.fcf_ap_fixed_trade_non_cust_invoiced
            ) as "Forecast Non-Customer Invoiced Fixed Trade {{currency_symbol}} (LIVE)",
            sum(
                ifnull(fcf_ap_tot_prime_cost_standard, 0)
                + ifnull(fcf_ap_tot_prime_cost_variance, 0)
            ) as "Forecast PCOS {{currency_symbol}} (LIVE) Act",
            sum(
                ifnull(fcf_ap_tot_prime_cost_standard, 0)
            ) as "Forecast PCOS {{currency_symbol}} (LIVE) Std",
            sum(
                ifnull(fcf_ap_permanent_disc, 0)
                + ifnull(fcf_ap_everyday_low_prices, 0)
                + ifnull(fcf_ap_early_settlement_disc, 0)
                + ifnull(fcf_ap_range_support_incentives, 0)
                + ifnull(fcf_ap_growth_incentives, 0)
            ) as "Forecast Permanent Discounts {{currency_symbol}}",
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
            ) as "Forecast Total Standard PCOS Amt {{currency_symbol}}",
            sum(
                case
                    when nvl(itm_ext.dummy_product_flag, 0) = 1
                    then 1 * (ifnull(fcf_ap_tot_prime_cost_variance, 0))
                end
            ) as "Forecast Total Var PCOS Amt {{currency_symbol}}",

            sum(
                ifnull(ly_{{currency_string}}_invoice_grs_amt, 0)
                + ifnull(ly_gl_{{currency_string}}_trade_avp * -1, 0)
            ) as "LY Gross Sales {{currency_symbol}}",
            sum(
                ifnull(ly_gl_{{currency_string}}_permd_rng_spt, 0)
                + ifnull(ly_gl_{{currency_string}}_permd_edlp, 0)
                + ifnull(ly_gl_{{currency_string}}_permd_csh_disc, 0)
                + ifnull(ly_gl_{{currency_string}}_permd_rsa_inct, 0)
            ) as "LY Permanent Discounts {{currency_symbol}}",
            sum(
                ifnull(
                    ifnull(ly_gl_{{currency_string}}_trade_retro, 0) + ifnull(ly_gl_{{currency_string}}_trade_avp, 0),
                    0
                ) + ifnull(
                    (
                        ifnull(ly_gl_{{currency_string}}_trade_promo_fixed, 0)
                        + ifnull(ly_gl_{{currency_string}}_trade_fxd_pymt, 0)
                        + ifnull(ly_gl_{{currency_string}}_trade_drct_shp, 0)
                        + ifnull(ly_gl_{{currency_string}}_trade_oth_drct_pymt, 0)
                    ),
                    0
                )
            ) as "LY Total Customer-Invoiced Trade {{currency_symbol}}",
            sum(
                ifnull(ly_gl_{{currency_string}}_trade_indrct_shp, 0)
                + ifnull(ly_gl_{{currency_string}}_trade_cat, 0)
                + ifnull(ly_gl_{{currency_string}}_trade_oth_indrct_pymt, 0)
                + ifnull(ly_gl_{{currency_string}}_trade_ag_cst, 0)
                + ifnull(ly_gl_{{currency_string}}_trade_other, 0)
            ) as "LY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
            sum(
                ifnull(ly_{{currency_string}}_ext_boughtin_amt, 0)
                + ifnull(ly_{{currency_string}}_ext_ing_amt, 0)
                + ifnull(ly_{{currency_string}}_ext_lbr_amt, 0)
                + ifnull(ly_{{currency_string}}_ext_oth_amt, 0)
                + ifnull(ly_{{currency_string}}_ext_pkg_amt, 0)
                + ifnull(ly_{{currency_string}}_ext_copack_amt, 0)
            ) as "LY Total Std PCOS Amt {{currency_symbol}}",
            sum(
                ifnull(ly_{{currency_string}}_pcos_raw_var, 0)
                + ifnull(ly_{{currency_string}}_pcos_copack_var, 0)
                + ifnull(ly_{{currency_string}}_pcos_labour_var, 0)
                + ifnull(ly_{{currency_string}}_pcos_other_var, 0)
                + ifnull(ly_{{currency_string}}_pcos_pack_var, 0)
                + ifnull(ly_{{currency_string}}_pcos_boughtin_var, 0)
            ) as "LY Total Var to Std PCOS Amt {{currency_symbol}}",
            sum(
                ifnull(ly_gl_{{currency_string}}_trade_retro, 0) + ifnull(ly_gl_{{currency_string}}_trade_avp, 0)
            ) as "LY Total Variable Trade {{currency_symbol}}",
            sum(
                (
                    ifnull(ly_gl_{{currency_string}}_trade_promo_fixed, 0)
                    + ifnull(ly_gl_{{currency_string}}_trade_fxd_pymt, 0)
                    + ifnull(ly_gl_{{currency_string}}_trade_drct_shp, 0)
                    + ifnull(ly_gl_{{currency_string}}_trade_oth_drct_pymt, 0)
                )
            ) as "LY Fixed Trade Customer-Invoiced (Combo Split)",
            sum(
                nvl(
                    nvl(ub.cy_{{currency_string}}_ext_boughtin_amt, 0)
                    + nvl(ub.cy_{{currency_string}}_ext_copack_amt, 0)
                    + nvl(ub.cy_{{currency_string}}_ext_ing_amt, 0)
                    + nvl(ub.cy_{{currency_string}}_ext_lbr_amt, 0)
                    + nvl(ub.cy_{{currency_string}}_ext_oth_amt, 0)
                    + nvl(ub.cy_{{currency_string}}_ext_pkg_amt, 0),
                    0
                ) + nvl(
                    nvl(ub.cy_{{currency_string}}_pcos_boughtin_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_copack_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_raw_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_labour_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_other_var, 0)
                    + nvl(ub.cy_{{currency_string}}_pcos_pack_var, 0),
                    0
                )
            ) as "CY Total PCOS Amt {{currency_symbol}}",
            sum(
                ifnull(cy_gl_{{currency_string}}_trade_retro, 0)
                + ifnull(cy_gl_{{currency_string}}_trade_avp, 0)
                + (
                    ifnull(cy_gl_{{currency_string}}_trade_promo_fixed, 0)
                    + ifnull(cy_gl_{{currency_string}}_trade_fxd_pymt, 0)
                    + ifnull(cy_gl_{{currency_string}}_trade_drct_shp, 0)
                    + ifnull(cy_gl_{{currency_string}}_trade_oth_drct_pymt, 0)
                )
            ) as "CY Total Customer-Invoiced Trade {{currency_symbol}}"

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
         --   on ub.source_system = cust_ext.source_system
            on ub.customer_addr_number_guid = cust_ext.customer_address_number_guid
         --   and nvl(ub.document_company, 'WBX') = cust_ext.company_code
        left join
            cust_planning plan
            on trim(ub.plan_source_customer_code) = trim(plan.trade_type_code)
            and ub.document_company = plan.company_code
        left join dim_date_cte dt on ub.calendar_date = dt.calendar_date
        left join
            scen_xref scen
            on upper(trim(scen.frozen_forecast)) = upper(trim(ub.frozen_forecast))
        where coalesce(cust_ext.trade_type_desc, plan.trade_type, '-') <> 'BULK'
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
    ),
    final as (
        select
            *,
            "CY Total Std PCOS Amt {{currency_symbol}}"
            + "Forecast PCOS (Std) {{currency_symbol}}" as "Cy PCOS Spend (Combo) Std",
            "CY Total Std PCOS Amt {{currency_symbol}}"
            + "CY Total Var to Std PCOS Amt {{currency_symbol}}"
            + "Forecast PCOS (Std) {{currency_symbol}}"
            + "Forecast PCOS (Var) {{currency_symbol}}" as "Cy PCOS Spend (Combo) Act",

            (
                (ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0)) + (
                    ifnull(
                        ("Cy AVP Discount {{currency_symbol}}" * -1) + "Forecast Added Value Pack {{currency_symbol}}", 0
                    )
                )
            ) + (
                ifnull(
                    "CY Permanent Discounts {{currency_symbol}}"
                    + (-1 * "Forecast Permanent Discounts {{currency_symbol}}"),
                    0
                )
            ) as "(Budget) Forecast Net Sales {{currency_symbol}} (Combo)",
            ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                + (-1 * "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"),
                0
            ) as "(Budget) Forecast Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)",
            (
                (
                    (
                        ifnull(
                            "Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0
                        )
                    ) + (
                        ifnull(
                            ("Cy AVP Discount {{currency_symbol}}" * -1) + "Forecast Added Value Pack {{currency_symbol}}",
                            0
                        )
                    )
                ) + (
                    ifnull(
                        "CY Permanent Discounts {{currency_symbol}}"
                        + (-1 * "Forecast Permanent Discounts {{currency_symbol}}"),
                        0
                    )
                )
            ) + ifnull(
                ifnull(
                    "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                    + (-1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}"),
                    0
                ),
                0
            ) as "(Budget) Forecast NRR {{currency_symbol}} (Combo)",
            ifnull(
                "CY Permanent Discounts {{currency_symbol}}" + (-1 * "Forecast Permanent Discounts {{currency_symbol}}"), 0
            ) as "(Budget) Forecast Permanent Discounts {{currency_symbol}} (Combo)",

            ifnull(
                "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                + (-1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}"),
                0
            ) as "(Budget) Forecast Total Customer-Invoiced Trade {{currency_symbol}} (Combo)",
            ifnull(
                "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                + (-1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}"),
                0
            )
            * -1 as "(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
            ifnull("CY Total Variable Trade {{currency_symbol}}" + "Budget Variable Trade Spend", 0)
            * -1 as "Budget Variable Trade {{currency_symbol}} (Combo)",

            -1 * ifnull(
                ("CY Total Variable Trade {{currency_symbol}}" + - "Forecast Total Variable Trade {{currency_symbol}}"), 0
            ) as "CY Customer-Invoiced Variable Trade {{currency_symbol}} (Combo Split)",

            "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}" + - (
                "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
            ) as "CY Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)",
            -1 * (
                "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                + - ("Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}")
            ) as "CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
            ifnull(
                "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                + - "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                0
            ) as "CY Total Customer-Invoiced Trade (Combo)",
            "CY Total Variable Trade {{currency_symbol}}"
            + - "Forecast Total Variable Trade {{currency_symbol}}"
            as "CY Total Variable Trade {{currency_symbol}} (Combo)",
            - ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
                0
            ) as "Forecast Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)",
            -1 * "CY Total PCOS Amt {{currency_symbol}}"
            + "Forecast PCOS (Std) {{currency_symbol}}"
            + "Forecast PCOS (Var) {{currency_symbol}}" as "Forecast PCOS {{currency_symbol}} (Combo) Act",
            -1 * "CY Total Std PCOS Amt {{currency_symbol}}"
            + "Forecast PCOS (Std) {{currency_symbol}}" as "Forecast PCOS {{currency_symbol}} (Combo) Std",
            "CY Permanent Discounts {{currency_symbol}}"
            - "Forecast Permanent Discounts {{currency_symbol}}"
            as "Forecast Permanent Discounts {{currency_symbol}} (Combo)"
        from pre_processing
    )

select
    budget_unique_key as budget_unique_key,
    "Branding",
    "Product Class",
    "Sub Product",
    "Packaging Size",
    market,
    submarket,
    trade_class,
    trade_group,
    trade_type,
    trade_sector,
    "Budget PCOS Var Flag",
    "calendar date",
    "Report fiscal year",
    "Report fiscal year period no",
    "Snapshot forecast date",
    "Frozen forecast",
    "Frozen forecast delineation date",
    "(Budget) Forecast Added Value Pack (Combo)"
    as "Budget (Budget) Forecast Added Value Pack (Combo)",
    "(Budget) Forecast Gross Amount (Combo)"
    as "Budget (Budget) Forecast Gross Amount (Combo)",
    "(Budget) Forecast Gross Sales {{currency_symbol}} (Combo)"
    as "Budget (Budget) Forecast Gross Sales {{currency_symbol}} (Combo)",
    "CY Added Value Pack {{currency_symbol}}" as "Budget CY Added Value Pack {{currency_symbol}}",
    "CY Total Customer-Invoiced Trade (Combo)"
    * -1 as "Budget CY Total Customer-Invoiced Trade Spend (Combo)",
    "CY Customer-Invoiced Fixed Trade {{currency_symbol}}" + (
        - "Forecast Customer-Invoiced Fixed Trade {{currency_symbol}}"
    ) as "Budget (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
    "Cy PCOS Spend (Combo) Std" as "Budget Cy PCOS Spend (Combo) Std",
    "Cy PCOS Spend (Combo) Act" as "Budget Cy PCOS Spend (Combo) Act",
    "(Budget) Forecast Net Sales {{currency_symbol}} (Combo)"
    as "Budget (Budget) Forecast Net Sales {{currency_symbol}} (Combo)",
    "(Budget) Forecast Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)"
    as "Budget (Budget) Forecast Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)",
    "(Budget) Forecast NRR {{currency_symbol}} (Combo)" as "Budget (Budget) Forecast NRR {{currency_symbol}} (Combo)",
    "(Budget) Forecast Permanent Discounts {{currency_symbol}} (Combo)"
    as "Budget (Budget) Forecast Permanent Discounts {{currency_symbol}} (Combo)",
    "(Budget) Forecast Total Customer-Invoiced Trade {{currency_symbol}} (Combo)"
    as "Budget (Budget) Forecast Total Customer-Invoiced Trade {{currency_symbol}} (Combo)",
    "(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)"
    as "Budget (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
    "Budget Variable Trade {{currency_symbol}} (Combo)" as "Budget Budget Variable Trade {{currency_symbol}} (Combo)",
    "CY Customer-Invoiced Variable Trade {{currency_symbol}} (Combo Split)"
    as "Budget CY Customer-Invoiced Variable Trade {{currency_symbol}} (Combo Split)",
    "CY Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)"
    as "Budget CY Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)",
    "CY Non-Customer Invoiced Fixed Trade Spend (Combo)"
    as "Budget CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
    "CY Total Customer-Invoiced Trade (Combo)"
    as "Budget CY Total Customer-Invoiced Trade (Combo)",
    "CY Total Variable Trade {{currency_symbol}} (Combo)" as "Budget CY Total Variable Trade {{currency_symbol}} (Combo)",
    "Forecast Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)"
    as "Budget Forecast Non-Customer Invoiced Fixed Trade {{currency_symbol}} (Combo)",
    "Forecast PCOS {{currency_symbol}} (Combo) Act" as "Budget Forecast PCOS {{currency_symbol}} (Combo) Act",
    "Forecast PCOS {{currency_symbol}} (Combo) Std" as "Budget Forecast PCOS {{currency_symbol}} (Combo) Std",
    "Forecast Permanent Discounts {{currency_symbol}} (Combo)"
    as "Budget Forecast Permanent Discounts {{currency_symbol}} (Combo)",
    ifnull(
        (
            (
                (
                    ifnull(
                        final."Cy Invoice Gross Amount {{currency_symbol}}"
                        + final."Forecast Gross Amount {{currency_symbol}}",
                        0
                    )
                ) + (
                    ifnull(
                        (final."Cy AVP Discount {{currency_symbol}}" * -1)
                        + final."Forecast Added Value Pack {{currency_symbol}}",
                        0
                    )
                )
            ) + (
                ifnull(
                    final."CY Permanent Discounts {{currency_symbol}}"
                    + (-1 * final."Forecast Permanent Discounts {{currency_symbol}}"),
                    0
                )
            )
        ) + ifnull(
            (
                ifnull(
                    final."CY Total Customer-Invoiced Trade {{currency_symbol}}"
                    + (-1 * final."Forecast Total Customer-Invoiced Trade {{currency_symbol}}"),
                    0
                )
            ),
            0
        ),
        0
    )
    + ifnull
    (
        (
            final."CY Total PCOS Amt {{currency_symbol}}" * -1
            + ifnull(final."Forecast PCOS (Std) {{currency_symbol}}", 0)
            + ifnull(final."Forecast PCOS (Var) {{currency_symbol}}", 0)
        ),
        0
    ) as "Budget (Budget) Forecast Gross Margin {{currency_symbol}} Act",
    ifnull(
        (
            (
                (
                    ifnull(
                        final."Cy Invoice Gross Amount {{currency_symbol}}"
                        + final."Forecast Gross Amount {{currency_symbol}}",
                        0
                    )
                ) + (
                    ifnull(
                        (final."Cy AVP Discount {{currency_symbol}}" * -1)
                        + final."Forecast Added Value Pack {{currency_symbol}}",
                        0
                    )
                )
            ) + (
                ifnull(
                    final."CY Permanent Discounts {{currency_symbol}}"
                    + (-1 * final."Forecast Permanent Discounts {{currency_symbol}}"),
                    0
                )
            )
        ) + ifnull(
            (
                ifnull(
                    final."CY Total Customer-Invoiced Trade {{currency_symbol}}"
                    + (-1 * final."Forecast Total Customer-Invoiced Trade {{currency_symbol}}"),
                    0
                )
            ),
            0
        ),
        0
    )
    + ifnull
    (
        (final."CY Total Std PCOS Amt {{currency_symbol}}" + ifnull(final."Forecast PCOS (Std) {{currency_symbol}}", 0))
        + (final."Forecast Total Standard PCOS Amt {{currency_symbol}}"),
        0
    ) as "Budget (Budget) Forecast Gross Margin {{currency_symbol}} Std",
    (
        ifnull(final."CY Total Std PCOS Amt {{currency_symbol}}", 0)
        + ifnull(final."CY Total Var to Std PCOS Amt {{currency_symbol}}", 0)
        + (
            ifnull(final."Forecast PCOS (Std) {{currency_symbol}}", 0)
            + ifnull(final."Forecast PCOS (Var) {{currency_symbol}}", 0)
        )
    ) + (
        -1 * (
            ifnull((final."Forecast Total Standard PCOS Amt {{currency_symbol}}"), 0)
            + ifnull((final."Forecast Total Var PCOS Amt {{currency_symbol}}"), 0)
        )
    ) as "(Budget) Forecast Total PCOS Amt {{currency_symbol}} Act PCOS",
    -1 * (-1 * final."CY Total Std PCOS Amt {{currency_symbol}}" + final."Forecast PCOS (Std) {{currency_symbol}}") + (
        final."Forecast Total Standard PCOS Amt {{currency_symbol}}" * -1
    ) as "Budget (Budget) Forecast Total PCOS Amt {{currency_symbol}} Std PCOS",
    ifnull(
        (
            - ifnull(
                final."CY Customer-Invoiced Fixed Trade {{currency_symbol}}"
                + - ifnull(final."Forecast Customer-Invoiced Fixed Trade {{currency_symbol}}", 0),
                0
            )
        ),
        0
    ) + (
        ifnull(
            final."CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
            + - final."Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
            0
        )
    )
    * -1 as "Budget Budget Fixed Trade Spend",
    -1 * (
        (
            final."CY Total PCOS Amt {{currency_symbol}}"
            + final."Forecast PCOS (Std) {{currency_symbol}}"
            + final."Forecast PCOS (Var) {{currency_symbol}}"
        ) + (
            -1 * final."Forecast Total Standard PCOS Amt {{currency_symbol}}" + nvl(
                case
                    when final."Budget PCOS Var Flag" = 1
                    then 1 * final."Forecast PCOS (Var) {{currency_symbol}}"
                    else 0
                end,
                0
            )
        )
    ) as "Budget Budget PCOS Spend Act",
    -1 * (
        (final."CY Total Std PCOS Amt {{currency_symbol}}" + ifnull(final."Forecast PCOS (Std) {{currency_symbol}}", 0))
        + (-1 * (final."Forecast Total Standard PCOS Amt {{currency_symbol}}"))
    ) as "Budget Budget PCOS Spend Std",
    case
        when "Budget Variable Trade {{currency_symbol}} (Combo)" = 0
        then 0
        else
            ifnull(
                (
                    (
                        - ifnull(
                            "CY Total Variable Trade {{currency_symbol}}"
                            + - ("Forecast Total Variable Trade {{currency_symbol}}"),
                            0
                        )
                    )
                    - ("Budget Variable Trade {{currency_symbol}} (Combo)")
                )
                / ("Budget Variable Trade {{currency_symbol}} (Combo)"),
                0
            )
    end as "Budget Customer-Invoiced Variable Trade % vs Budget (Split)",
    case
        when
            nvl(
                (
                    "CY Total Variable Trade {{currency_symbol}}"
                    + ("Forecast Total Variable Trade {{currency_symbol}}" * -1)
                ),
                0
            )
            = 0
        then 0
        else
            ifnull(
                (
                    (
                        - ifnull(
                            "CY Total Variable Trade {{currency_symbol}}"
                            + - ("Forecast Total Variable Trade {{currency_symbol}}"),
                            0
                        )
                    ) - (
                        - ifnull(
                            "CY Total Variable Trade {{currency_symbol}}"
                            + - ("Forecast Total Variable Trade {{currency_symbol}}"),
                            0
                        )
                    )
                ) / nvl(
                    (
                        "CY Total Variable Trade {{currency_symbol}}"
                        + ("Forecast Total Variable Trade {{currency_symbol}}" * -1)
                    ),
                    0
                )
                * -1,
                0
            )
    end as "Budget Customer-Invoiced Variable Trade % vs Forecast (Split)",
    case
        when "LY Total Variable Trade {{currency_symbol}}" = 0
        then 0
        else
            ifnull(
                (
                    - ifnull(
                        "CY Total Variable Trade {{currency_symbol}}"
                        + - ("Forecast Total Variable Trade {{currency_symbol}}"),
                        0
                    )
                    - (- "LY Total Variable Trade {{currency_symbol}}")
                )
                / (- "LY Total Variable Trade {{currency_symbol}}"),
                0
            )
    end as "Budget Customer-Invoiced Variable Trade % vs LY (Split)",
    ifnull(
        - ifnull("CY Total Variable Trade {{currency_symbol}}" + - ("Forecast Total Variable Trade {{currency_symbol}}"), 0)
        - - ifnull(
            "CY Total Variable Trade {{currency_symbol}}"
            + (ifnull("Forecast Total Variable Trade {{currency_symbol}}", 0) * -1),
            0
        ),
        0
    ) as "Budget Customer-Invoiced Variable Trade Var to Budget (Split)",

    ifnull(
        - ifnull("CY Total Variable Trade {{currency_symbol}}" + - ("Forecast Total Variable Trade {{currency_symbol}}"), 0)
        - - ifnull(
            "CY Total Variable Trade {{currency_symbol}}" + - "Forecast Total Variable Trade {{currency_symbol}}", 0
        ),
        0
    ) as "Budget Customer-Invoiced Variable Trade Var to Forecast (Split)",

    ifnull(
        - ifnull("CY Total Variable Trade {{currency_symbol}}" + - ("Forecast Total Variable Trade {{currency_symbol}}"), 0)
        - (- "LY Total Variable Trade {{currency_symbol}}"),
        0
    ) as "Budget Customer-Invoiced Variable Trade Var to LY (Split)",
    ifnull(
        (
            (
                (ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0))
                + (ifnull("Cy AVP Discount {{currency_symbol}}" * -1 + "Forecast Added Value Pack {{currency_symbol}}", 0))
            ) + (
                ifnull(
                    "CY Permanent Discounts {{currency_symbol}}" + -1 * "Forecast Permanent Discounts {{currency_symbol}}",
                    0
                )
            )
        ),
        0
    ) + ifnull(
        (
            ifnull(
                "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                + - "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                0
            )
        ),
        0
    ) as "Budget CY NRR {{currency_symbol}} (Combo)",
    final."CY Total Variable Trade {{currency_symbol}}"
    + - final."Forecast Total Variable Trade {{currency_symbol}}"
    * -1 as "Budget Cy Variable Trade Spend",
    ifnull(
        "CY Permanent Discounts {{currency_symbol}}" + -1 * "Forecast Permanent Discounts {{currency_symbol}}", 0
    ) as "Budget Forecast 1 Permanent Discounts {{currency_symbol}} (Combo)",
    ifnull(
        "CY Added Value Pack {{currency_symbol}}" + "Forecast Added Value Pack {{currency_symbol}}",
        0
    ) as "Budget Forecast Added Value Pack (Combo)", - (
        - ifnull(
            "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
            + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
            0
        )
    )
    + ifnull(
        "CY Customer-Invoiced Fixed Trade {{currency_symbol}}"
        + - ifnull("Forecast Customer-Invoiced Fixed Trade {{currency_symbol}}", 0),
        0
    ) as "Budget Forecast Fixed Trade {{currency_symbol}} (Combo)",
    ifnull(
        "CY Customer-Invoiced Fixed Trade {{currency_symbol}}"
        + - "Forecast Customer-Invoiced Fixed Trade {{currency_symbol}}",
        0
    ) as "Budget Forecast Fixed Trade Customer-Invoiced",
    -1 * (

        - (
            - ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
                0
            )
        )
        + ifnull(
            "CY Customer-Invoiced Fixed Trade {{currency_symbol}}"
            + - "Forecast Customer-Invoiced Fixed Trade {{currency_symbol}}",
            0
        )
    ) as "Budget Forecast Fixed Trade Spend",
    (
        (
            (
                ((ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0)))
                + (ifnull("CY Added Value Pack {{currency_symbol}}" + "Forecast Added Value Pack {{currency_symbol}}", 0))
            ) + (
                ifnull(
                    "CY Permanent Discounts {{currency_symbol}}" + - "Forecast Permanent Discounts {{currency_symbol}}", 0
                )
            )
        ) + (
            ifnull(
                (
                    ifnull(
                        "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                        + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                        0
                    )
                ),
                0
            )
        )
    ) - (
        ifnull(
            (
                ifnull(
                    - "CY Total PCOS Amt {{currency_symbol}}"
                    + ifnull("Forecast PCOS (Std) {{currency_symbol}}", 0)
                    + "Forecast PCOS (Var) {{currency_symbol}}",
                    0
                )
            ),
            0
        )
    )
    - (
        ifnull(
            - ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
                0
            ),
            0
        )
    ) as "Budget Forecast GCAT {{currency_symbol}} (Combo) Act",
    (
        (
            (
                ((ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0)))
                + 0  -- ((budget) forecast added value pack(combo)) field not found
            ) + (
                ifnull(
                    "CY Permanent Discounts {{currency_symbol}}" + - "Forecast Permanent Discounts {{currency_symbol}}", 0
                )
            )
        ) + (
            ifnull(
                (
                    ifnull(
                        "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                        + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                        0
                    )
                ),
                0
            )
        )
    ) - (
        ifnull(
            (
                ifnull(
                    - "CY Total Std PCOS Amt {{currency_symbol}}" + ifnull("Forecast PCOS (Std) {{currency_symbol}}", 0), 0
                )
            ),
            0
        )
    )
    - (
        ifnull(
            - ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
                0
            ),
            0
        )
    ) as "Budget Forecast GCAT {{currency_symbol}} (Combo) Std",
    ifnull(
        "Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0
    ) as "Budget Forecast Gross Amount (Combo)",
    ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0)
    + "(Budget) Forecast Added Value Pack (Combo)" as "Forecast Gross Sales {{currency_symbol}} (Combo)",
    "Forecast Gross Sales {{currency_symbol}} (Combo)" + (
        ifnull("CY Permanent Discounts {{currency_symbol}}" + - "Forecast Permanent Discounts {{currency_symbol}}", 0)
    ) as "Budget Forecast Net Sales {{currency_symbol}} (Combo)",
    ifnull(
        "CY Total Customer-Invoiced Trade {{currency_symbol}}"
        + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
        0
    ) as "Budget Forecast Total Customer-Invoiced Trade {{currency_symbol}} (Combo)",
    -1 * ifnull(
        "CY Total Customer-Invoiced Trade {{currency_symbol}}"
        + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
        0
    ) as "Budget Forecast Total Customer-Invoiced Trade Spend (Combo)",
    - ifnull(
        "CY Total Variable Trade {{currency_symbol}}" + - "Forecast Total Variable Trade {{currency_symbol}}", 0
    ) as "Budget Forecast Variable Trade {{currency_symbol}} (Combo) (1)",
    - ifnull(
        "CY Total Variable Trade {{currency_symbol}}" + - "Forecast Total Variable Trade {{currency_symbol}}", 0
    ) as "Budget Forecast Variable Trade Spend",
    "LY Gross Sales {{currency_symbol}}" + "LY Permanent Discounts {{currency_symbol}}" as "Ly Net Sales (Combo)",
    ifnull("Ly Net Sales (Combo)", 0) + ifnull(
        ifnull(
            "LY Total Customer-Invoiced Trade {{currency_symbol}}"
            + - "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
            0
        ),
        0
    ) as "Budget LY NRR {{currency_symbol}} (Combo)",
    ifnull(
        "LY Total Customer-Invoiced Trade {{currency_symbol}}"
        + - "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
        0
    ) as "Budget LY Total Customer-Invoiced Trade (Combo)",
    abs(
        (
            -1 * (
                ifnull(
                    "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                    + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                    0
                )
            )
        ) - (
            -1 * (
                ifnull(
                    "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                    + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                    0
                )
            )
        )
    ) as "Budget Abs Var Forecast Cust-Inv Trade",
    (
        ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0)
        + (ifnull("CY Added Value Pack {{currency_symbol}}" + "Forecast Added Value Pack {{currency_symbol}}", 0))
    ) + (
        ifnull("CY Permanent Discounts {{currency_symbol}}" + -1 * "Forecast Permanent Discounts {{currency_symbol}}", 0)
    ) as "Forecast 1 Net Sales {{currency_symbol}} (Combo)",

    abs(
        (
            ifnull(
                (
                    ifnull("Forecast 1 Net Sales {{currency_symbol}} (Combo)", 0) + ifnull(
                        (
                            ifnull(
                                "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                                + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                                0
                            )
                        ),
                        0
                    )
                ),
                0
            ) - ifnull(
                ifnull(
                    - "CY Total Std PCOS Amt {{currency_symbol}}" + ifnull("Forecast PCOS (Std) {{currency_symbol}}", 0), 0
                ),
                0
            )
            + ifnull(
                ifnull(
                    "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                    + - "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
                    0
                ),
                0
            )
        ) - (
            ifnull(
                ifnull(
                    ifnull(
                        ifnull(
                            "Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0
                        ),
                        0
                    ) + ifnull(
                        ifnull(
                            "CY Added Value Pack {{currency_symbol}}" + "Forecast Added Value Pack {{currency_symbol}}", 0
                        ),
                        0
                    )
                    + ifnull(
                        ifnull(
                            "CY Permanent Discounts {{currency_symbol}}"
                            + -1 * "Forecast Permanent Discounts {{currency_symbol}}",
                            0
                        ),
                        0
                    ),
                    0
                ) + ifnull(
                    ifnull(
                        "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                        + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                        0
                    ),
                    0
                ),
                0
            ) - ifnull(
                ifnull(
                    - "CY Total Std PCOS Amt {{currency_symbol}}" + ifnull("Forecast PCOS (Std) {{currency_symbol}}", 0), 0
                ),
                0
            )
            + ifnull(
                ifnull(
                    "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                    + - "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
                    0
                ),
                0
            )
        )
    ) as "Budget Abs Var Forecast GCAT Std",

    abs(
        (
            (
                ifnull("Forecast 1 Net Sales {{currency_symbol}} (Combo)", 0) + ifnull(
                    (
                        ifnull(
                            "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                            + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                            0
                        )
                    ),
                    0
                )
            )
        ) - (
            ifnull(
                ifnull(
                    ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0),
                    0
                ) + ifnull(
                    ifnull("CY Added Value Pack {{currency_symbol}}" + "Forecast Added Value Pack {{currency_symbol}}", 0),
                    0
                )
                + ifnull(
                    ifnull(
                        "CY Permanent Discounts {{currency_symbol}}"
                        + -1 * "Forecast Permanent Discounts {{currency_symbol}}",
                        0
                    ),
                    0
                ),
                0
            ) + ifnull(
                ifnull(
                    "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                    + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                    0
                ),
                0
            )
        )
    ) as "Budget Abs Var Forecast NRR",

    abs(
        (
            ifnull(
                (
                    ifnull("Forecast 1 Net Sales {{currency_symbol}} (Combo)", 0) + ifnull(
                        (
                            ifnull(
                                "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                                + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                                0
                            )
                        ),
                        0
                    )
                ),
                0
            ) - ifnull(
                ifnull(
                    - "CY Total PCOS Amt {{currency_symbol}}"
                    + ifnull("Forecast PCOS (Std) {{currency_symbol}}", 0)
                    + ifnull("Forecast PCOS (Var) {{currency_symbol}}", 0),
                    0
                ),
                0
            )
            + ifnull(
                ifnull(
                    "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                    + - "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
                    0
                ),
                0
            )
        ) - (
            ifnull(
                ifnull(
                    ifnull(
                        ifnull(
                            "Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0
                        ),
                        0
                    ) + ifnull(
                        ifnull(
                            "CY Added Value Pack {{currency_symbol}}" + "Forecast Added Value Pack {{currency_symbol}}", 0
                        ),
                        0
                    )
                    + ifnull(
                        ifnull(
                            "CY Permanent Discounts {{currency_symbol}}"
                            + -1 * "Forecast Permanent Discounts {{currency_symbol}}",
                            0
                        ),
                        0
                    ),
                    0
                ) + ifnull(
                    ifnull(
                        "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                        + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                        0
                    ),
                    0
                ),
                0
            ) - ifnull(
                ifnull(
                    - "CY Total PCOS Amt {{currency_symbol}}"
                    + ifnull("Forecast PCOS (Std) {{currency_symbol}}", 0)
                    + ifnull("Forecast PCOS (Var) {{currency_symbol}}", 0),
                    0
                ),
                0
            )
            + ifnull(
                ifnull(
                    "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
                    + - "Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}",
                    0
                ),
                0
            )
        )
    ) as "Budget Abs Var Forecast GCAT Act",
    ifnull(
        ifnull(
            (
                ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0)
                + ifnull(("Cy AVP Discount {{currency_symbol}}" * -1) + "Forecast Added Value Pack {{currency_symbol}}", 0)
            ) + (
                ifnull(
                    "CY Permanent Discounts {{currency_symbol}}"
                    + (-1 * "Forecast Permanent Discounts {{currency_symbol}}"),
                    0
                )
            ),
            0
        ) + ifnull(
            (
                ifnull(
                    "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                    + - ("Forecast Total Customer-Invoiced Trade {{currency_symbol}}"),
                    0
                )
            ),
            0
        ),
        0
    ) - ifnull(
        (
            - (
                ifnull("CY Total Std PCOS Amt {{currency_symbol}}", 0)
                + ifnull("CY Total Var to Std PCOS Amt {{currency_symbol}}", 0)
            )
            + ifnull("Forecast PCOS (Std) {{currency_symbol}}", 0)
            + ifnull("Forecast PCOS (Var) {{currency_symbol}}", 0)
        ),
        0
    )
    + ifnull(
        (
            "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
            + - ("Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}")
        ),
        0
    ) as "Budget CY GCAT {{currency_symbol}} (Combo) Act",
    ifnull(
        ifnull(

            ifnull("Cy Invoice Gross Amount {{currency_symbol}}" + "Forecast Gross Amount {{currency_symbol}}", 0)
            + ifnull(("Cy AVP Discount {{currency_symbol}}" * -1) + "Forecast Added Value Pack {{currency_symbol}}", 0)
            + (
                ifnull(
                    "CY Permanent Discounts {{currency_symbol}}"
                    + (-1 * "Forecast Permanent Discounts {{currency_symbol}}"),
                    0
                )
            ),
            0
        ) + ifnull(
            (
                ifnull(
                    "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                    + - ("Forecast Total Customer-Invoiced Trade {{currency_symbol}}"),
                    0
                )
            ),
            0
        ),
        0
    )
    - ifnull((- "CY Total Std PCOS Amt {{currency_symbol}}" + ifnull("Forecast PCOS (Std) {{currency_symbol}}", 0)), 0)
    + ifnull(
        (
            "CY Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}"
            + - ("Forecast Total Non-Customer Invoiced Fixed Trade {{currency_symbol}}")
        ),
        0
    ) as "Budget CY GCAT {{currency_symbol}} (Combo) Std",
    "Budget Forecast Net Sales {{currency_symbol}} (Combo)" + (
        ifnull(
            ifnull(
                "CY Total Customer-Invoiced Trade {{currency_symbol}}"
                + -1 * "Forecast Total Customer-Invoiced Trade {{currency_symbol}}",
                0
            ),
            0
        )
    ) as "Budget Forecast NRR {{currency_symbol}} (Combo)"

from final
