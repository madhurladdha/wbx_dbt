{{
    config(
        materialized=env_var("DBT_MAT_VIEW"),
        tags=["sales", "performance", "sales_performance_lite"],
    )
}}

with
    dim_date_cte as (select * from {{ ref("src_dim_date") }}),
    scen_xref as (select * from {{ ref("src_sls_wtx_budget_scen_xref") }}),
    uber as (
        select *
        from {{ ref("fct_wbx_sls_uber") }}
        where
            (nvl(document_company, '-')) in ('WBX', '-')
            and nvl(source_content_filter, '-') not in ('EPOS', 'TERMS', '-')
            and calendar_date >= dateadd('month', -27, date_trunc('year', current_date))
    ),
    /* Pick Actuals prior to last week and forecast after last week */
    uber_cte as (
        select
            source_system,
            source_item_identifier,
            source_customer_code,
            plan_source_customer_code,
            customer_addr_number_guid,
            calendar_date,
            fiscal_period_number,
            ly_calendar_date,
            snapshot_forecast_date,
            primary_uom,
            source_content_filter,
            cy_line_actual_ship_date,
            cy_scheduled_ship_date,
            cy_ordered_ca_quantity,
            cy_shipped_ca_quantity,
            cy_backord_ca_quantity,
            cy_cancel_ca_quantity,
            cy_short_ca_quantity,
            cy_sales_ca_quantity,
            cy_ordered_ul_quantity,
            cy_shipped_ul_quantity,
            cy_backord_ul_quantity,
            cy_cancel_ul_quantity,
            cy_short_ul_quantity,
            cy_sales_ul_quantity,
            cy_ordered_kg_quantity,
            cy_shipped_kg_quantity,
            cy_backord_kg_quantity,
            cy_cancel_kg_quantity,
            cy_short_kg_quantity,
            cy_sales_kg_quantity,
            cy_base_rpt_grs_amt,
            cy_base_invoice_grs_amt,
            cy_base_ext_ing_amt,
            cy_base_ext_pkg_amt,
            cy_base_ext_copack_amt,
            cy_base_ext_lbr_amt,
            cy_base_ext_oth_amt,
            cy_base_ext_boughtin_amt,
            ly_line_actual_ship_date,
            ly_scheduled_ship_date,
            ly_ordered_ca_quantity,
            ly_shipped_ca_quantity,
            ly_backord_ca_quantity,
            ly_cancel_ca_quantity,
            ly_short_ca_quantity,
            ly_sales_ca_quantity,
            ly_ordered_ul_quantity,
            ly_shipped_ul_quantity,
            ly_backord_ul_quantity,
            ly_cancel_ul_quantity,
            ly_short_ul_quantity,
            ly_sales_ul_quantity,
            ly_ordered_kg_quantity,
            ly_shipped_kg_quantity,
            ly_backord_kg_quantity,
            ly_cancel_kg_quantity,
            ly_short_kg_quantity,
            ly_sales_kg_quantity,
            ly_trans_rpt_grs_amt,
            ly_trans_rpt_net_amt,
            ly_base_rpt_grs_amt,
            ly_base_rpt_net_amt,
            ly_phi_rpt_grs_amt,
            ly_phi_rpt_net_amt,
            ly_pcomp_rpt_grs_amt,
            ly_pcomp_rpt_net_amt,
            ly_trans_rpt_grs_price,
            ly_trans_rpt_net_price,
            ly_base_rpt_grs_ca_price,
            ly_base_rpt_net_ca_price,
            ly_phi_rpt_grs_ca_price,
            ly_phi_rpt_net_ca_price,
            ly_pcomp_rpt_grs_ca_price,
            ly_pcomp_rpt_net_ca_price,
            ly_base_ext_ing_cost,
            ly_base_ext_pkg_cost,
            ly_base_ext_boughtin_cost,
            ly_base_ext_copack_cost,
            ly_base_ext_oth_cost,
            ly_base_ext_lbr_cost,
            ly_trans_invoice_grs_amt,
            ly_trans_invoice_net_amt,
            ly_base_invoice_grs_amt,
            ly_base_invoice_net_amt,
            ly_phi_invoice_grs_amt,
            ly_phi_invoice_net_amt,
            ly_pcomp_invoice_grs_amt,
            ly_pcomp_invoice_net_amt,
            ly_base_ext_ing_amt,
            ly_base_ext_pkg_amt,
            ly_base_ext_boughtin_amt,
            ly_base_ext_copack_amt,
            ly_base_ext_oth_amt,
            ly_base_ext_lbr_amt,
            frozen_forecast,
            budget_amount,
            cy_trans_pcos_labour_var,
            cy_base_pcos_labour_var,
            cy_base_pcos_raw_var,
            cy_base_pcos_pack_var,
            ly_base_pcos_copack_var,
            document_company,
            ly_base_pcos_boughtin_var,
            ly_base_pcos_other_var,
            ly_base_pcos_pack_var,
            ly_base_pcos_raw_var,
            ly_base_pcos_labour_var,
            cy_base_pcos_boughtin_var,
            cy_base_pcos_copack_var,
            cy_base_pcos_other_var,
            cy_gl_base_trade_ag_cst,
            cy_gl_base_permd_csh_disc,
            cy_gl_base_trade_avp,
            cy_gl_base_permd_edlp,
            cy_gl_base_permd_rng_spt,
            cy_gl_base_permd_rsa_inct,
            cy_gl_base_trade_other,
            ly_gl_base_trade_avp,
            ly_gl_base_trade_ag_cst,
            ly_gl_base_permd_csh_disc,
            ly_gl_base_permd_edlp,
            ly_gl_base_permd_rng_spt,
            ly_gl_base_permd_rsa_inct,
            ly_gl_base_trade_other,
            cy_gl_base_trade_drct_shp,
            cy_gl_base_trade_indrct_shp,
            cy_gl_base_trade_oth_drct_pymt,
            cy_gl_base_trade_oth_indrct_pymt,
            cy_gl_base_trade_promo_fixed,
            cy_gl_base_trade_fxd_pymt,
            cy_gl_base_trade_retro,
            ly_gl_base_trade_drct_shp,
            ly_gl_base_trade_indrct_shp,
            ly_gl_base_trade_oth_drct_pymt,
            ly_gl_base_trade_cat,
            ly_gl_base_trade_oth_indrct_pymt,
            cy_gl_base_trade_cat,
            ly_gl_base_trade_promo_fixed,
            ly_gl_base_trade_fxd_pymt,
            ly_gl_base_trade_retro,
            fcf_tot_vol_sp_base_uom,
            fcf_tot_vol_ca,
            fcf_tot_vol_kg,
            fcf_tot_vol_ul,
            fcf_ap_added_value_pack,
            fcf_ap_fixed_trade_cust_invoiced,
            fcf_ap_variable_trade,
            fcf_ap_net_sales_value,
            fcf_ap_permanent_disc,
            fcf_ap_range_support_incentives,
            fcf_ap_total_trade_cust_invoiced,
            fcf_ap_fixed_trade_non_cust_invoiced,
            fcf_ap_everyday_low_prices,
            fcf_ap_off_invoice_disc_pre_adjustment,
            fcf_ap_off_invoice_disc_mgmt_adjustment,
            fcf_ap_off_invoice_disc,
            fcf_ap_field_marketing_pre_adjustment,
            fcf_ap_field_marketing_mgmt_adjustment,
            fcf_ap_field_marketing,
            fcf_ap_tot_prime_cost_variance_pre_adjustment,
            fcf_ap_tot_prime_cost_variance_mgmt_adjustment,
            fcf_ap_tot_prime_cost_variance,
            fcf_ap_tot_prime_cost_standard_pre_adjustment,
            fcf_ap_tot_prime_cost_standard_mgmt_adjustment,
            fcf_ap_tot_prime_cost_standard,
            fcf_ap_early_settlement_disc_pre_adjustment,
            fcf_ap_early_settlement_disc_mgmt_adjustment,
            fcf_ap_early_settlement_disc,
            fcf_ap_other_direct_payments_pre_adjustment,
            fcf_ap_gross_selling_value,
            fcf_ap_gross_sales_value,
            fcf_ap_growth_incentives,
            fcf_ap_tot_prime_cost_standard_raw,
            fcf_ap_tot_prime_cost_standard_packaging,
            fcf_ap_tot_prime_cost_standard_labour,
            fcf_ap_tot_prime_cost_standard_bought_in,
            fcf_ap_tot_prime_cost_standard_other,
            fcf_ap_tot_prime_cost_standard_co_pack
        from uber
        where
            snapshot_forecast_date in (
                select distinct calendar_date
                from dim_date_cte
                where
                    calendar_date = (
                        select to_date(calendar_week_end_dt) as current_week_dt
                        from dim_date_cte
                        where calendar_date = current_date - 14
                    )
            )
            and calendar_date >= (
                select distinct calendar_date
                from dim_date_cte
                where
                    calendar_date = (
                        select to_date(calendar_week_end_dt) as current_week_dt
                        from dim_date_cte
                        where calendar_date = current_date - 14
                    )
            )
            and source_content_filter = 'FORECAST'
        union
        select
            source_system,
            source_item_identifier,
            source_customer_code,
            plan_source_customer_code,
            customer_addr_number_guid,
            calendar_date,
            fiscal_period_number,
            ly_calendar_date,
            snapshot_forecast_date,
            primary_uom,
            source_content_filter,
            cy_line_actual_ship_date,
            cy_scheduled_ship_date,
            cy_ordered_ca_quantity,
            cy_shipped_ca_quantity,
            cy_backord_ca_quantity,
            cy_cancel_ca_quantity,
            cy_short_ca_quantity,
            cy_sales_ca_quantity,
            cy_ordered_ul_quantity,
            cy_shipped_ul_quantity,
            cy_backord_ul_quantity,
            cy_cancel_ul_quantity,
            cy_short_ul_quantity,
            cy_sales_ul_quantity,
            cy_ordered_kg_quantity,
            cy_shipped_kg_quantity,
            cy_backord_kg_quantity,
            cy_cancel_kg_quantity,
            cy_short_kg_quantity,
            cy_sales_kg_quantity,
            cy_base_rpt_grs_amt,
            cy_base_invoice_grs_amt,
            cy_base_ext_ing_amt,
            cy_base_ext_pkg_amt,
            cy_base_ext_copack_amt,
            cy_base_ext_lbr_amt,
            cy_base_ext_oth_amt,
            cy_base_ext_boughtin_amt,
            ly_line_actual_ship_date,
            ly_scheduled_ship_date,
            ly_ordered_ca_quantity,
            ly_shipped_ca_quantity,
            ly_backord_ca_quantity,
            ly_cancel_ca_quantity,
            ly_short_ca_quantity,
            ly_sales_ca_quantity,
            ly_ordered_ul_quantity,
            ly_shipped_ul_quantity,
            ly_backord_ul_quantity,
            ly_cancel_ul_quantity,
            ly_short_ul_quantity,
            ly_sales_ul_quantity,
            ly_ordered_kg_quantity,
            ly_shipped_kg_quantity,
            ly_backord_kg_quantity,
            ly_cancel_kg_quantity,
            ly_short_kg_quantity,
            ly_sales_kg_quantity,
            ly_trans_rpt_grs_amt,
            ly_trans_rpt_net_amt,
            ly_base_rpt_grs_amt,
            ly_base_rpt_net_amt,
            ly_phi_rpt_grs_amt,
            ly_phi_rpt_net_amt,
            ly_pcomp_rpt_grs_amt,
            ly_pcomp_rpt_net_amt,
            ly_trans_rpt_grs_price,
            ly_trans_rpt_net_price,
            ly_base_rpt_grs_ca_price,
            ly_base_rpt_net_ca_price,
            ly_phi_rpt_grs_ca_price,
            ly_phi_rpt_net_ca_price,
            ly_pcomp_rpt_grs_ca_price,
            ly_pcomp_rpt_net_ca_price,
            ly_base_ext_ing_cost,
            ly_base_ext_pkg_cost,
            ly_base_ext_boughtin_cost,
            ly_base_ext_copack_cost,
            ly_base_ext_oth_cost,
            ly_base_ext_lbr_cost,
            ly_trans_invoice_grs_amt,
            ly_trans_invoice_net_amt,
            ly_base_invoice_grs_amt,
            ly_base_invoice_net_amt,
            ly_phi_invoice_grs_amt,
            ly_phi_invoice_net_amt,
            ly_pcomp_invoice_grs_amt,
            ly_pcomp_invoice_net_amt,
            ly_base_ext_ing_amt,
            ly_base_ext_pkg_amt,
            ly_base_ext_boughtin_amt,
            ly_base_ext_copack_amt,
            ly_base_ext_oth_amt,
            ly_base_ext_lbr_amt,
            frozen_forecast,
            budget_amount,
            cy_trans_pcos_labour_var,
            cy_base_pcos_labour_var,
            cy_base_pcos_raw_var,
            cy_base_pcos_pack_var,
            ly_base_pcos_copack_var,
            document_company,
            ly_base_pcos_boughtin_var,
            ly_base_pcos_other_var,
            ly_base_pcos_pack_var,
            ly_base_pcos_raw_var,
            ly_base_pcos_labour_var,
            cy_base_pcos_boughtin_var,
            cy_base_pcos_copack_var,
            cy_base_pcos_other_var,
            cy_gl_base_trade_ag_cst,
            cy_gl_base_permd_csh_disc,
            cy_gl_base_trade_avp,
            cy_gl_base_permd_edlp,
            cy_gl_base_permd_rng_spt,
            cy_gl_base_permd_rsa_inct,
            cy_gl_base_trade_other,
            ly_gl_base_trade_avp,
            ly_gl_base_trade_ag_cst,
            ly_gl_base_permd_csh_disc,
            ly_gl_base_permd_edlp,
            ly_gl_base_permd_rng_spt,
            ly_gl_base_permd_rsa_inct,
            ly_gl_base_trade_other,
            cy_gl_base_trade_drct_shp,
            cy_gl_base_trade_indrct_shp,
            cy_gl_base_trade_oth_drct_pymt,
            cy_gl_base_trade_oth_indrct_pymt,
            cy_gl_base_trade_promo_fixed,
            cy_gl_base_trade_fxd_pymt,
            cy_gl_base_trade_retro,
            ly_gl_base_trade_drct_shp,
            ly_gl_base_trade_indrct_shp,
            ly_gl_base_trade_oth_drct_pymt,
            ly_gl_base_trade_cat,
            ly_gl_base_trade_oth_indrct_pymt,
            cy_gl_base_trade_cat,
            ly_gl_base_trade_promo_fixed,
            ly_gl_base_trade_fxd_pymt,
            ly_gl_base_trade_retro,
            fcf_tot_vol_sp_base_uom,
            fcf_tot_vol_ca,
            fcf_tot_vol_kg,
            fcf_tot_vol_ul,
            fcf_ap_added_value_pack,
            fcf_ap_fixed_trade_cust_invoiced,
            fcf_ap_variable_trade,
            fcf_ap_net_sales_value,
            fcf_ap_permanent_disc,
            fcf_ap_range_support_incentives,
            fcf_ap_total_trade_cust_invoiced,
            fcf_ap_fixed_trade_non_cust_invoiced,
            fcf_ap_everyday_low_prices,
            fcf_ap_off_invoice_disc_pre_adjustment,
            fcf_ap_off_invoice_disc_mgmt_adjustment,
            fcf_ap_off_invoice_disc,
            fcf_ap_field_marketing_pre_adjustment,
            fcf_ap_field_marketing_mgmt_adjustment,
            fcf_ap_field_marketing,
            fcf_ap_tot_prime_cost_variance_pre_adjustment,
            fcf_ap_tot_prime_cost_variance_mgmt_adjustment,
            fcf_ap_tot_prime_cost_variance,
            fcf_ap_tot_prime_cost_standard_pre_adjustment,
            fcf_ap_tot_prime_cost_standard_mgmt_adjustment,
            fcf_ap_tot_prime_cost_standard,
            fcf_ap_early_settlement_disc_pre_adjustment,
            fcf_ap_early_settlement_disc_mgmt_adjustment,
            fcf_ap_early_settlement_disc,
            fcf_ap_other_direct_payments_pre_adjustment,
            fcf_ap_gross_selling_value,
            fcf_ap_gross_sales_value,
            fcf_ap_growth_incentives,
            fcf_ap_tot_prime_cost_standard_raw,
            fcf_ap_tot_prime_cost_standard_packaging,
            fcf_ap_tot_prime_cost_standard_labour,
            fcf_ap_tot_prime_cost_standard_bought_in,
            fcf_ap_tot_prime_cost_standard_other,
            fcf_ap_tot_prime_cost_standard_co_pack
        from uber
        where
            calendar_date in (
                select distinct calendar_date
                from dim_date_cte
                where
                    calendar_date < (
                        select to_date(calendar_week_end_dt) as current_week_dt
                        from dim_date_cte
                        where calendar_date = current_date - 14
                    )
            )
            and source_content_filter in (
                'ACTUALS',
                'GENERAL_LEDGER_PCOS_PPV',
                'GENERAL_LEDGER_DNI',
                'GENERAL_LEDGER_PCOS_STD_JOURNAL',
                'GENERAL_LEDGER_TRADE'
            )

        union

        select
            source_system,
            source_item_identifier,
            source_customer_code,
            plan_source_customer_code,
            customer_addr_number_guid,
            calendar_date,
            fiscal_period_number,
            ly_calendar_date,
            snapshot_forecast_date,
            primary_uom,
            source_content_filter,
            null as cy_line_actual_ship_date,
            null as cy_scheduled_ship_date,
            0 as cy_ordered_ca_quantity,
            0 as cy_shipped_ca_quantity,
            0 as cy_backord_ca_quantity,
            0 as cy_cancel_ca_quantity,
            0 as cy_short_ca_quantity,
            0 as cy_sales_ca_quantity,
            0 as cy_ordered_ul_quantity,
            0 as cy_shipped_ul_quantity,
            0 as cy_backord_ul_quantity,
            0 as cy_cancel_ul_quantity,
            0 as cy_short_ul_quantity,
            0 as cy_sales_ul_quantity,
            0 as cy_ordered_kg_quantity,
            0 as cy_shipped_kg_quantity,
            0 as cy_backord_kg_quantity,
            0 as cy_cancel_kg_quantity,
            0 as cy_short_kg_quantity,
            0 as cy_sales_kg_quantity,
            0 as cy_base_rpt_grs_amt,
            0 as cy_base_invoice_grs_amt,
            0 as cy_base_ext_ing_amt,
            0 as cy_base_ext_pkg_amt,
            0 as cy_base_ext_copack_amt,
            0 as cy_base_ext_lbr_amt,
            0 as cy_base_ext_oth_amt,
            0 as cy_base_ext_boughtin_amt,
            null as ly_line_actual_ship_date,
            null as ly_scheduled_ship_date,
            0 as ly_ordered_ca_quantity,
            0 as ly_shipped_ca_quantity,
            0 as ly_backord_ca_quantity,
            0 as ly_cancel_ca_quantity,
            0 as ly_short_ca_quantity,
            0 as ly_sales_ca_quantity,
            0 as ly_ordered_ul_quantity,
            0 as ly_shipped_ul_quantity,
            0 as ly_backord_ul_quantity,
            0 as ly_cancel_ul_quantity,
            0 as ly_short_ul_quantity,
            0 as ly_sales_ul_quantity,
            0 as ly_ordered_kg_quantity,
            0 as ly_shipped_kg_quantity,
            0 as ly_backord_kg_quantity,
            0 as ly_cancel_kg_quantity,
            0 as ly_short_kg_quantity,
            0 as ly_sales_kg_quantity,
            0 as ly_trans_rpt_grs_amt,
            0 as ly_trans_rpt_net_amt,
            0 as ly_base_rpt_grs_amt,
            0 as ly_base_rpt_net_amt,
            0 as ly_phi_rpt_grs_amt,
            0 as ly_phi_rpt_net_amt,
            0 as ly_pcomp_rpt_grs_amt,
            0 as ly_pcomp_rpt_net_amt,
            0 as ly_trans_rpt_grs_price,
            0 as ly_trans_rpt_net_price,
            0 as ly_base_rpt_grs_ca_price,
            0 as ly_base_rpt_net_ca_price,
            0 as ly_phi_rpt_grs_ca_price,
            0 as ly_phi_rpt_net_ca_price,
            0 as ly_pcomp_rpt_grs_ca_price,
            0 as ly_pcomp_rpt_net_ca_price,
            0 as ly_base_ext_ing_cost,
            0 as ly_base_ext_pkg_cost,
            0 as ly_base_ext_boughtin_cost,
            0 as ly_base_ext_copack_cost,
            0 as ly_base_ext_oth_cost,
            0 as ly_base_ext_lbr_cost,
            0 as ly_trans_invoice_grs_amt,
            0 as ly_trans_invoice_net_amt,
            0 as ly_base_invoice_grs_amt,
            0 as ly_base_invoice_net_amt,
            0 as ly_phi_invoice_grs_amt,
            0 as ly_phi_invoice_net_amt,
            0 as ly_pcomp_invoice_grs_amt,
            0 as ly_pcomp_invoice_net_amt,
            0 as ly_base_ext_ing_amt,
            0 as ly_base_ext_pkg_amt,
            0 as ly_base_ext_boughtin_amt,
            0 as ly_base_ext_copack_amt,
            0 as ly_base_ext_oth_amt,
            0 as ly_base_ext_lbr_amt,
            frozen_forecast as frozen_forecast,
            0 as budget_amount,
            0 as cy_trans_pcos_labour_var,
            0 as cy_base_pcos_labour_var,
            0 as cy_base_pcos_raw_var,
            0 as cy_base_pcos_pack_var,
            0 as ly_base_pcos_copack_var,
            '' as document_company,
            0 as ly_base_pcos_boughtin_var,
            0 as ly_base_pcos_other_var,
            0 as ly_base_pcos_pack_var,
            0 as ly_base_pcos_raw_var,
            0 as ly_base_pcos_labour_var,
            0 as cy_base_pcos_boughtin_var,
            0 as cy_base_pcos_copack_var,
            0 as cy_base_pcos_other_var,
            0 as cy_gl_base_trade_ag_cst,
            0 as cy_gl_base_permd_csh_disc,
            0 as cy_gl_base_trade_avp,
            0 as cy_gl_base_permd_edlp,
            0 as cy_gl_base_permd_rng_spt,
            0 as cy_gl_base_permd_rsa_inct,
            0 as cy_gl_base_trade_other,
            0 as ly_gl_base_trade_avp,
            0 as ly_gl_base_trade_ag_cst,
            0 as ly_gl_base_permd_csh_disc,
            0 as ly_gl_base_permd_edlp,
            0 as ly_gl_base_permd_rng_spt,
            0 as ly_gl_base_permd_rsa_inct,
            0 as ly_gl_base_trade_other,
            0 as cy_gl_base_trade_drct_shp,
            0 as cy_gl_base_trade_indrct_shp,
            0 as cy_gl_base_trade_oth_drct_pymt,
            0 as cy_gl_base_trade_oth_indrct_pymt,
            0 as cy_gl_base_trade_promo_fixed,
            0 as cy_gl_base_trade_fxd_pymt,
            0 as cy_gl_base_trade_retro,
            0 as ly_gl_base_trade_drct_shp,
            0 as ly_gl_base_trade_indrct_shp,
            0 as ly_gl_base_trade_oth_drct_pymt,
            0 as ly_gl_base_trade_cat,
            0 as ly_gl_base_trade_oth_indrct_pymt,
            0 as cy_gl_base_trade_cat,
            0 as ly_gl_base_trade_promo_fixed,
            0 as ly_gl_base_trade_fxd_pymt,
            0 as ly_gl_base_trade_retro,
            0 as fcf_tot_vol_sp_base_uom,
            0 as fcf_tot_vol_ca,
            0 as fcf_tot_vol_kg,
            0 as fcf_tot_vol_ul,
            fcf_ap_added_value_pack as fcf_ap_added_value_pack,
            fcf_ap_fixed_trade_cust_invoiced as fcf_ap_fixed_trade_cust_invoiced,
            fcf_ap_variable_trade as fcf_ap_variable_trade,
            0 as fcf_ap_net_sales_value,
            fcf_ap_permanent_disc as fcf_ap_permanent_disc,
            fcf_ap_range_support_incentives as fcf_ap_range_support_incentives,
            fcf_ap_total_trade_cust_invoiced as fcf_ap_total_trade_cust_invoiced,
            fcf_ap_fixed_trade_non_cust_invoiced
            as fcf_ap_fixed_trade_non_cust_invoiced,
            fcf_ap_everyday_low_prices as fcf_ap_everyday_low_prices,
            0 as fcf_ap_off_invoice_disc_pre_adjustment,
            0 as fcf_ap_off_invoice_disc_mgmt_adjustment,
            0 as fcf_ap_off_invoice_disc,
            0 as fcf_ap_field_marketing_pre_adjustment,
            0 as fcf_ap_field_marketing_mgmt_adjustment,
            0 as fcf_ap_field_marketing,
            0 as fcf_ap_tot_prime_cost_variance_pre_adjustment,
            0 as fcf_ap_tot_prime_cost_variance_mgmt_adjustment,
            0 as fcf_ap_tot_prime_cost_variance,
            0 as fcf_ap_tot_prime_cost_standard_pre_adjustment,
            0 as fcf_ap_tot_prime_cost_standard_mgmt_adjustment,
            0 as fcf_ap_tot_prime_cost_standard,
            fcf_ap_early_settlement_disc as fcf_ap_early_settlement_disc_pre_adjustment,
            0 as fcf_ap_early_settlement_disc_mgmt_adjustment,
            0 as fcf_ap_early_settlement_disc,
            0 as fcf_ap_other_direct_payments_pre_adjustment,
            0 as fcf_ap_gross_selling_value,
            0 as fcf_ap_gross_sales_value,
            fcf_ap_growth_incentives as fcf_ap_growth_incentives,
            0 as fcf_ap_tot_prime_cost_standard_raw,
            0 as fcf_ap_tot_prime_cost_standard_packaging,
            0 as fcf_ap_tot_prime_cost_standard_labour,
            0 as fcf_ap_tot_prime_cost_standard_bought_in,
            0 as fcf_ap_tot_prime_cost_standard_other,
            0 as fcf_ap_tot_prime_cost_standard_co_pack
        from uber
        where
            (nvl(document_company, '-')) in ('WBX', '-')
            and nvl(source_content_filter, '-') not in ('EPOS', 'TERMS', '-')
            and calendar_date >= date_trunc('MONTH', current_date)
            and calendar_date < (
                select to_date(calendar_week_end_dt) as current_week_dt
                from dim_date_cte
                where calendar_date = current_date - 14
            )
            and source_content_filter = 'FORECAST'
            and snapshot_forecast_date = (
                select distinct calendar_date
                from dim_date_cte
                where
                    calendar_date = (
                        select to_date(calendar_week_end_dt) as current_week_dt
                        from dim_date_cte
                        where calendar_date = current_date - 14
                    )
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
            as lastweek_unique_key,
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
                nvl(ub.cy_gl_base_trade_avp, 0) * -1
                + nvl(ub.fcf_ap_added_value_pack, 0)
            ) as "(Budget) Forecast Added Value Pack (Combo)",
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
                nvl(cy_base_invoice_grs_amt, 0) + nvl(fcf_ap_gross_selling_value, 0)
            ) as "Forecast 1 Gross Amount £ (Combo)",
            sum(
                nvl(ub.fcf_ap_fixed_trade_non_cust_invoiced, 0)
            ) as "Forecast Non-Customer Invoiced Fixed Trade £ (LIVE)",
            sum(
                ifnull(fcf_ap_tot_prime_cost_standard, 0)
                + ifnull(fcf_ap_tot_prime_cost_variance, 0)
            ) as "Forecast PCOS £ (LIVE) Act",
            sum(
                ifnull(fcf_ap_tot_prime_cost_standard, 0)
            ) as "Forecast PCOS £ (LIVE) Std",
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

        from uber_cte ub
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
          --  and nvl(ub.document_company, 'WBX') = cust_ext.company_code
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
            "CY Total Std PCOS Amt £"
            + "Forecast PCOS (Std) £" as "Cy PCOS Spend (Combo) Std",
            "CY Total Std PCOS Amt £"
            + "CY Total Var to Std PCOS Amt £"
            + "Forecast PCOS (Std) £"
            + "Forecast PCOS (Var) £" as "Cy PCOS Spend (Combo) Act",

            (
                (ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)) + (
                    ifnull(
                        ("Cy AVP Discount £" * -1) + "Forecast Added Value Pack £", 0
                    )
                )
            ) + (
                ifnull(
                    "CY Permanent Discounts £"
                    + (-1 * "Forecast Permanent Discounts £"),
                    0
                )
            ) as "(Budget) Forecast Net Sales £ (Combo)",
            ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade £"
                + (-1 * "Forecast Total Non-Customer Invoiced Fixed Trade £"),
                0
            ) as "(Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            (
                (
                    (
                        ifnull(
                            "Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0
                        )
                    ) + (
                        ifnull(
                            ("Cy AVP Discount £" * -1) + "Forecast Added Value Pack £",
                            0
                        )
                    )
                ) + (
                    ifnull(
                        "CY Permanent Discounts £"
                        + (-1 * "Forecast Permanent Discounts £"),
                        0
                    )
                )
            ) + ifnull(
                ifnull(
                    "CY Total Customer-Invoiced Trade £"
                    + (-1 * "Forecast Total Customer-Invoiced Trade £"),
                    0
                ),
                0
            ) as "(Budget) Forecast NRR £ (Combo)",
            ifnull(
                "CY Permanent Discounts £" + (-1 * "Forecast Permanent Discounts £"), 0
            ) as "(Budget) Forecast Permanent Discounts £ (Combo)",

            ifnull(
                "CY Total Customer-Invoiced Trade £"
                + (-1 * "Forecast Total Customer-Invoiced Trade £"),
                0
            ) as "(Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
            ifnull(
                "CY Total Customer-Invoiced Trade £"
                + (-1 * "Forecast Total Customer-Invoiced Trade £"),
                0
            )
            * -1 as "(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",

            ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade £"
                + - "Forecast Total Non-Customer Invoiced Fixed Trade £",
                0
            ) as "(budget)Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            ifnull("CY Total Variable Trade £" + "Budget Variable Trade Spend", 0)
            * -1 as "Budget Variable Trade £ (Combo)",

            -1 * ifnull(
                ("CY Total Variable Trade £" + - "Forecast Total Variable Trade £"), 0
            ) as "CY Customer-Invoiced Variable Trade £ (Combo Split)",

            "CY Total Non-Customer Invoiced Fixed Trade £" + - (
                "Forecast Total Non-Customer Invoiced Fixed Trade £"
            ) as "CY Non-Customer Invoiced Fixed Trade £ (Combo)",
            -1 * (
                "CY Total Non-Customer Invoiced Fixed Trade £"
                + - ("Forecast Total Non-Customer Invoiced Fixed Trade £")
            ) as "CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
            ifnull(
                "CY Total Customer-Invoiced Trade £"
                + - "Forecast Total Customer-Invoiced Trade £",
                0
            ) as "CY Total Customer-Invoiced Trade (Combo)",
            "CY Total Variable Trade £"
            + - "Forecast Total Variable Trade £"
            as "CY Total Variable Trade £ (Combo)",
            - ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade £"
                + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade £",
                0
            ) as "Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
            -1 * "CY Total PCOS Amt £"
            + "Forecast PCOS (Std) £"
            + "Forecast PCOS (Var) £" as "Forecast PCOS £ (Combo) Act",
            -1 * "CY Total Std PCOS Amt £"
            + "Forecast PCOS (Std) £" as "Forecast PCOS £ (Combo) Std",
            "CY Permanent Discounts £"
            - "Forecast Permanent Discounts £"
            as "Forecast Permanent Discounts £ (Combo)"
        from pre_processing
    )

select
    lastweek_unique_key as lw_unique_key,
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
    as "LW (Budget) Forecast Added Value Pack (Combo)",
    "(Budget) Forecast Gross Amount (Combo)"
    as "LW Budget (Budget) Forecast Gross Amount (Combo)",
    "(Budget) Forecast Gross Sales £ (Combo)"
    as "LW (Budget) Forecast Gross Sales £ (Combo)",
    "CY Added Value Pack £" as "LW CY Added Value Pack £",
    "CY Total Customer-Invoiced Trade (Combo)"
    * -1 as "LW CY Total Customer-Invoiced Trade Spend (Combo)",
    "CY Customer-Invoiced Fixed Trade £" + (
        - "Forecast Customer-Invoiced Fixed Trade £"
    ) as "LW (Budget) Fixed Trade Customer-Invoiced Var to Budge (Split)",
    "Cy PCOS Spend (Combo) Std" as "LW Cy PCOS Spend (Combo) Std",
    "Cy PCOS Spend (Combo) Act" as "LW Cy PCOS Spend (Combo) Act",
    "(Budget) Forecast Net Sales £ (Combo)"
    as "LW (Budget) Forecast Net Sales £ (Combo)",
    "(Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"
    as "LW (Budget) Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    "(Budget) Forecast NRR £ (Combo)" as "LW (Budget) Forecast NRR £ (Combo)",
    "(Budget) Forecast Permanent Discounts £ (Combo)"
    as "LW (Budget) Forecast Permanent Discounts £ (Combo)",
    "(Budget) Forecast Total Customer-Invoiced Trade £ (Combo)"
    as "LW (Budget) Forecast Total Customer-Invoiced Trade £ (Combo)",
    "(Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)"
    as "LW (Budget) Forecast Total Customer-Invoiced Trade Spend (Combo)",
    "Budget Variable Trade £ (Combo)" as "LW Budget Variable Trade £ (Combo)",
    "CY Customer-Invoiced Variable Trade £ (Combo Split)"
    as "LW CY Customer-Invoiced Variable Trade £ (Combo Split)",
    "CY Non-Customer Invoiced Fixed Trade £ (Combo)"
    as "LW CY Non-Customer Invoiced Fixed Trade £ (Combo)",
    "CY Non-Customer Invoiced Fixed Trade Spend (Combo)"
    as "LW CY Non-Customer Invoiced Fixed Trade Spend (Combo)",
    "CY Total Customer-Invoiced Trade (Combo)"
    as "LW CY Total Customer-Invoiced Trade (Combo)",
    "CY Total Variable Trade £ (Combo)" as "LW CY Total Variable Trade £ (Combo)",
    "Forecast Non-Customer Invoiced Fixed Trade £ (Combo)"
    as "LW Forecast Non-Customer Invoiced Fixed Trade £ (Combo)",
    "Forecast PCOS £ (Combo) Act" as "LW Forecast PCOS £ (Combo) Act",
    "Forecast PCOS £ (Combo) Std" as "LW Forecast PCOS £ (Combo) Std",
    "Forecast Permanent Discounts £ (Combo)"
    as "LW Forecast Permanent Discounts £ (Combo)",
    ifnull(
        (
            (
                (
                    ifnull(
                        final."Cy Invoice Gross Amount £"
                        + final."Forecast Gross Amount £",
                        0
                    )
                ) + (
                    ifnull(
                        (final."Cy AVP Discount £" * -1)
                        + final."Forecast Added Value Pack £",
                        0
                    )
                )
            ) + (
                ifnull(
                    final."CY Permanent Discounts £"
                    + (-1 * final."Forecast Permanent Discounts £"),
                    0
                )
            )
        ) + ifnull(
            (
                ifnull(
                    final."CY Total Customer-Invoiced Trade £"
                    + (-1 * final."Forecast Total Customer-Invoiced Trade £"),
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
            final."CY Total PCOS Amt £" * -1
            + ifnull(final."Forecast PCOS (Std) £", 0)
            + ifnull(final."Forecast PCOS (Var) £", 0)
        ),
        0
    ) as "LW (Budget) Forecast Gross Margin £ Act",
    ifnull(
        (
            (
                (
                    ifnull(
                        final."Cy Invoice Gross Amount £"
                        + final."Forecast Gross Amount £",
                        0
                    )
                ) + (
                    ifnull(
                        (final."Cy AVP Discount £" * -1)
                        + final."Forecast Added Value Pack £",
                        0
                    )
                )
            ) + (
                ifnull(
                    final."CY Permanent Discounts £"
                    + (-1 * final."Forecast Permanent Discounts £"),
                    0
                )
            )
        ) + ifnull(
            (
                ifnull(
                    final."CY Total Customer-Invoiced Trade £"
                    + (-1 * final."Forecast Total Customer-Invoiced Trade £"),
                    0
                )
            ),
            0
        ),
        0
    )
    + ifnull
    (
        (final."CY Total Std PCOS Amt £" + ifnull(final."Forecast PCOS (Std) £", 0))
        + (final."Forecast Total Standard PCOS Amt £"),
        0
    ) as "LW (Budget) Forecast Gross Margin £ Std",
    (
        ifnull(final."CY Total Std PCOS Amt £", 0)
        + ifnull(final."CY Total Var to Std PCOS Amt £", 0)
        + (
            ifnull(final."Forecast PCOS (Std) £", 0)
            + ifnull(final."Forecast PCOS (Var) £", 0)
        )
    ) + (
        -1 * (
            ifnull((final."Forecast Total Standard PCOS Amt £"), 0)
            + ifnull((final."Forecast Total Var PCOS Amt £"), 0)
        )
    ) as "(Budget) Forecast Total PCOS Amt £ Act PCOS",
    -1 * (-1 * final."CY Total Std PCOS Amt £" + final."Forecast PCOS (Std) £") + (
        final."Forecast Total Standard PCOS Amt £" * -1
    ) as "LW (Budget) Forecast Total PCOS Amt £ Std PCOS",
    ifnull(
        (
            - ifnull(
                final."CY Customer-Invoiced Fixed Trade £"
                + - ifnull(final."Forecast Customer-Invoiced Fixed Trade £", 0),
                0
            )
        ),
        0
    ) + (
        ifnull(
            final."CY Total Non-Customer Invoiced Fixed Trade £"
            + - final."Forecast Total Non-Customer Invoiced Fixed Trade £",
            0
        )
    )
    * -1 as "LW Budget Fixed Trade Spend",
    -1 * (
        (
            final."CY Total PCOS Amt £"
            + final."Forecast PCOS (Std) £"
            + final."Forecast PCOS (Var) £"
        ) + (
            -1 * final."Forecast Total Standard PCOS Amt £" + nvl(
                case
                    when final."Budget PCOS Var Flag" = 1
                    then 1 * final."Forecast PCOS (Var) £"
                    else 0
                end,
                0
            )
        )
    ) as "LW Budget PCOS Spend Act",
    -1 * (
        (final."CY Total Std PCOS Amt £" + ifnull(final."Forecast PCOS (Std) £", 0))
        + (-1 * (final."Forecast Total Standard PCOS Amt £"))
    ) as "LW Budget PCOS Spend Std",
    case
        when "Budget Variable Trade £ (Combo)" = 0
        then 0
        else
            ifnull(
                (
                    (
                        - ifnull(
                            "CY Total Variable Trade £"
                            + - ("Forecast Total Variable Trade £"),
                            0
                        )
                    )
                    - ("Budget Variable Trade £ (Combo)")
                )
                / ("Budget Variable Trade £ (Combo)"),
                0
            )
    end as "LW Customer-Invoiced Variable Trade % vs Budget (Split)",
    case
        when
            nvl(
                (
                    "CY Total Variable Trade £"
                    + ("Forecast Total Variable Trade £" * -1)
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
                            "CY Total Variable Trade £"
                            + - ("Forecast Total Variable Trade £"),
                            0
                        )
                    ) - (
                        - ifnull(
                            "CY Total Variable Trade £"
                            + - ("Forecast Total Variable Trade £"),
                            0
                        )
                    )
                ) / nvl(
                    (
                        "CY Total Variable Trade £"
                        + ("Forecast Total Variable Trade £" * -1)
                    ),
                    0
                )
                * -1,
                0
            )
    end as "LW Customer-Invoiced Variable Trade % vs Forecast (Split)",
    case
        when "LY Total Variable Trade £" = 0
        then 0
        else
            ifnull(
                (
                    - ifnull(
                        "CY Total Variable Trade £"
                        + - ("Forecast Total Variable Trade £"),
                        0
                    )
                    - (- "LY Total Variable Trade £")
                )
                / (- "LY Total Variable Trade £"),
                0
            )
    end as "LW Customer-Invoiced Variable Trade % vs LY (Split)",
    ifnull(
        - ifnull("CY Total Variable Trade £" + - ("Forecast Total Variable Trade £"), 0)
        - - ifnull(
            "CY Total Variable Trade £"
            + (ifnull("Forecast Total Variable Trade £", 0) * -1),
            0
        ),
        0
    ) as "LW Customer-Invoiced Variable Trade Var to Budget (Split)",

    ifnull(
        - ifnull("CY Total Variable Trade £" + - ("Forecast Total Variable Trade £"), 0)
        - - ifnull(
            "CY Total Variable Trade £" + - "Forecast Total Variable Trade £", 0
        ),
        0
    ) as "LW Customer-Invoiced Variable Trade Var to Forecast (Split)",

    ifnull(
        - ifnull("CY Total Variable Trade £" + - ("Forecast Total Variable Trade £"), 0)
        - (- "LY Total Variable Trade £"),
        0
    ) as "LW Customer-Invoiced Variable Trade Var to LY (Split)",
    ifnull(
        (
            (
                (ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0))
                + (ifnull("Cy AVP Discount £" * -1 + "Forecast Added Value Pack £", 0))
            ) + (
                ifnull(
                    "CY Permanent Discounts £" + -1 * "Forecast Permanent Discounts £",
                    0
                )
            )
        ),
        0
    ) + ifnull(
        (
            ifnull(
                "CY Total Customer-Invoiced Trade £"
                + - "Forecast Total Customer-Invoiced Trade £",
                0
            )
        ),
        0
    ) as "LW CY NRR £ (Combo)",
    final."CY Total Variable Trade £"
    + - final."Forecast Total Variable Trade £" * -1 as "LW Cy Variable Trade Spend",
    ifnull(
        "CY Permanent Discounts £" + -1 * "Forecast Permanent Discounts £", 0
    ) as "LW Forecast 1 Permanent Discounts £ (Combo)",
    ifnull(
        "CY Added Value Pack £" + "Forecast Added Value Pack £",
        0
    ) as "LW Forecast Added Value Pack (Combo)", - (
        - ifnull(
            "CY Total Non-Customer Invoiced Fixed Trade £"
            + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade £",
            0
        )
    )
    + ifnull(
        "CY Customer-Invoiced Fixed Trade £"
        + - ifnull("Forecast Customer-Invoiced Fixed Trade £", 0),
        0
    ) as "LW Forecast Fixed Trade £ (Combo)",
    ifnull(
        "CY Customer-Invoiced Fixed Trade £"
        + - "Forecast Customer-Invoiced Fixed Trade £",
        0
    ) as "LW Forecast Fixed Trade Customer-Invoiced",
    -1 * (

        - (
            - ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade £"
                + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade £",
                0
            )
        )
        + ifnull(
            "CY Customer-Invoiced Fixed Trade £"
            + - "Forecast Customer-Invoiced Fixed Trade £",
            0
        )
    ) as "LW Forecast Fixed Trade Spend",
    (
        (
            (
                ((ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)))
                + (ifnull("CY Added Value Pack £" + "Forecast Added Value Pack £", 0))
            ) + (
                ifnull(
                    "CY Permanent Discounts £" + - "Forecast Permanent Discounts £", 0
                )
            )
        ) + (
            ifnull(
                (
                    ifnull(
                        "CY Total Customer-Invoiced Trade £"
                        + -1 * "Forecast Total Customer-Invoiced Trade £",
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
                    - "CY Total PCOS Amt £"
                    + ifnull("Forecast PCOS (Std) £", 0)
                    + "Forecast PCOS (Var) £",
                    0
                )
            ),
            0
        )
    )
    - (
        ifnull(
            - ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade £"
                + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade £",
                0
            ),
            0
        )
    ) as "LW Forecast GCAT £ (Combo) Act",
    (
        (
            (
                ((ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)))
                + 0  -- ((budget) forecast added value pack(combo)) field not found
            ) + (
                ifnull(
                    "CY Permanent Discounts £" + - "Forecast Permanent Discounts £", 0
                )
            )
        ) + (
            ifnull(
                (
                    ifnull(
                        "CY Total Customer-Invoiced Trade £"
                        + -1 * "Forecast Total Customer-Invoiced Trade £",
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
                    - "CY Total Std PCOS Amt £" + ifnull("Forecast PCOS (Std) £", 0), 0
                )
            ),
            0
        )
    )
    - (
        ifnull(
            - ifnull(
                "CY Total Non-Customer Invoiced Fixed Trade £"
                + -1 * "Forecast Total Non-Customer Invoiced Fixed Trade £",
                0
            ),
            0
        )
    ) as "LW Forecast GCAT £ (Combo) Std",
    ifnull(
        "Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0
    ) as "LW Forecast Gross Amount (Combo)",
    ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)
    + "(Budget) Forecast Added Value Pack (Combo)" as "Forecast Gross Sales £ (Combo)",
    "Forecast Gross Sales £ (Combo)" + (
        ifnull("CY Permanent Discounts £" + - "Forecast Permanent Discounts £", 0)
    ) as "LW Forecast Net Sales £ (Combo)",
    ifnull(
        "CY Total Customer-Invoiced Trade £"
        + -1 * "Forecast Total Customer-Invoiced Trade £",
        0
    ) as "LW Forecast Total Customer-Invoiced Trade £ (Combo)",
    -1 * ifnull(
        "CY Total Customer-Invoiced Trade £"
        + -1 * "Forecast Total Customer-Invoiced Trade £",
        0
    ) as "LW Forecast Total Customer-Invoiced Trade Spend (Combo)",
    - ifnull(
        "CY Total Variable Trade £" + - "Forecast Total Variable Trade £", 0
    ) as "LW Forecast Variable Trade £ (Combo) (1)",
    - ifnull(
        "CY Total Variable Trade £" + - "Forecast Total Variable Trade £", 0
    ) as "LW Forecast Variable Trade Spend",
    "LY Gross Sales £" + "LY Permanent Discounts £" as "Ly Net Sales (Combo)",
    ifnull("Ly Net Sales (Combo)", 0) + ifnull(
        ifnull(
            "LY Total Customer-Invoiced Trade £"
            + - "Forecast Total Customer-Invoiced Trade £",
            0
        ),
        0
    ) as "LW LY NRR £ (Combo)",
    ifnull(
        "LY Total Customer-Invoiced Trade £"
        + - "Forecast Total Customer-Invoiced Trade £",
        0
    ) as "LW LY Total Customer-Invoiced Trade (Combo)",
    abs(
        (
            -1 * (
                ifnull(
                    "CY Total Customer-Invoiced Trade £"
                    + -1 * "Forecast Total Customer-Invoiced Trade £",
                    0
                )
            )
        ) - (
            -1 * (
                ifnull(
                    "CY Total Customer-Invoiced Trade £"
                    + -1 * "Forecast Total Customer-Invoiced Trade £",
                    0
                )
            )
        )
    ) as "LW Abs Var Forecast Cust-Inv Trade",
    (
        ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)
        + (ifnull("CY Added Value Pack £" + "Forecast Added Value Pack £", 0))
    ) + (
        ifnull("CY Permanent Discounts £" + -1 * "Forecast Permanent Discounts £", 0)
    ) as "Forecast 1 Net Sales £ (Combo)",

    abs(
        (
            ifnull(
                (
                    ifnull("Forecast 1 Net Sales £ (Combo)", 0) + ifnull(
                        (
                            ifnull(
                                "CY Total Customer-Invoiced Trade £"
                                + -1 * "Forecast Total Customer-Invoiced Trade £",
                                0
                            )
                        ),
                        0
                    )
                ),
                0
            ) - ifnull(
                ifnull(
                    - "CY Total Std PCOS Amt £" + ifnull("Forecast PCOS (Std) £", 0), 0
                ),
                0
            )
            + ifnull(
                ifnull(
                    "CY Total Non-Customer Invoiced Fixed Trade £"
                    + - "Forecast Total Non-Customer Invoiced Fixed Trade £",
                    0
                ),
                0
            )
        ) - (
            ifnull(
                ifnull(
                    ifnull(
                        ifnull(
                            "Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0
                        ),
                        0
                    ) + ifnull(
                        ifnull(
                            "CY Added Value Pack £" + "Forecast Added Value Pack £", 0
                        ),
                        0
                    )
                    + ifnull(
                        ifnull(
                            "CY Permanent Discounts £"
                            + -1 * "Forecast Permanent Discounts £",
                            0
                        ),
                        0
                    ),
                    0
                ) + ifnull(
                    ifnull(
                        "CY Total Customer-Invoiced Trade £"
                        + -1 * "Forecast Total Customer-Invoiced Trade £",
                        0
                    ),
                    0
                ),
                0
            ) - ifnull(
                ifnull(
                    - "CY Total Std PCOS Amt £" + ifnull("Forecast PCOS (Std) £", 0), 0
                ),
                0
            )
            + ifnull(
                ifnull(
                    "CY Total Non-Customer Invoiced Fixed Trade £"
                    + - "Forecast Total Non-Customer Invoiced Fixed Trade £",
                    0
                ),
                0
            )
        )
    ) as "LW Abs Var Forecast GCAT Std",

    abs(
        (
            (
                ifnull("Forecast 1 Net Sales £ (Combo)", 0) + ifnull(
                    (
                        ifnull(
                            "CY Total Customer-Invoiced Trade £"
                            + -1 * "Forecast Total Customer-Invoiced Trade £",
                            0
                        )
                    ),
                    0
                )
            )
        ) - (
            ifnull(
                ifnull(
                    ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0),
                    0
                ) + ifnull(
                    ifnull("CY Added Value Pack £" + "Forecast Added Value Pack £", 0),
                    0
                )
                + ifnull(
                    ifnull(
                        "CY Permanent Discounts £"
                        + -1 * "Forecast Permanent Discounts £",
                        0
                    ),
                    0
                ),
                0
            ) + ifnull(
                ifnull(
                    "CY Total Customer-Invoiced Trade £"
                    + -1 * "Forecast Total Customer-Invoiced Trade £",
                    0
                ),
                0
            )
        )
    ) as "LW Abs Var Forecast NRR",

    abs(
        (
            ifnull(
                (
                    ifnull("Forecast 1 Net Sales £ (Combo)", 0) + ifnull(
                        (
                            ifnull(
                                "CY Total Customer-Invoiced Trade £"
                                + -1 * "Forecast Total Customer-Invoiced Trade £",
                                0
                            )
                        ),
                        0
                    )
                ),
                0
            ) - ifnull(
                ifnull(
                    - "CY Total PCOS Amt £"
                    + ifnull("Forecast PCOS (Std) £", 0)
                    + ifnull("Forecast PCOS (Var) £", 0),
                    0
                ),
                0
            )
            + ifnull(
                ifnull(
                    "CY Total Non-Customer Invoiced Fixed Trade £"
                    + - "Forecast Total Non-Customer Invoiced Fixed Trade £",
                    0
                ),
                0
            )
        ) - (
            ifnull(
                ifnull(
                    ifnull(
                        ifnull(
                            "Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0
                        ),
                        0
                    ) + ifnull(
                        ifnull(
                            "CY Added Value Pack £" + "Forecast Added Value Pack £", 0
                        ),
                        0
                    )
                    + ifnull(
                        ifnull(
                            "CY Permanent Discounts £"
                            + -1 * "Forecast Permanent Discounts £",
                            0
                        ),
                        0
                    ),
                    0
                ) + ifnull(
                    ifnull(
                        "CY Total Customer-Invoiced Trade £"
                        + -1 * "Forecast Total Customer-Invoiced Trade £",
                        0
                    ),
                    0
                ),
                0
            ) - ifnull(
                ifnull(
                    - "CY Total PCOS Amt £"
                    + ifnull("Forecast PCOS (Std) £", 0)
                    + ifnull("Forecast PCOS (Var) £", 0),
                    0
                ),
                0
            )
            + ifnull(
                ifnull(
                    "CY Total Non-Customer Invoiced Fixed Trade £"
                    + - "Forecast Total Non-Customer Invoiced Fixed Trade £",
                    0
                ),
                0
            )
        )
    ) as "LW Abs Var Forecast GCAT Act",
    ifnull(
        ifnull(
            (
                ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)
                + ifnull(("Cy AVP Discount £" * -1) + "Forecast Added Value Pack £", 0)
            ) + (
                ifnull(
                    "CY Permanent Discounts £"
                    + (-1 * "Forecast Permanent Discounts £"),
                    0
                )
            ),
            0
        ) + ifnull(
            (
                ifnull(
                    "CY Total Customer-Invoiced Trade £"
                    + - ("Forecast Total Customer-Invoiced Trade £"),
                    0
                )
            ),
            0
        ),
        0
    ) - ifnull(
        (
            - (
                ifnull("CY Total Std PCOS Amt £", 0)
                + ifnull("CY Total Var to Std PCOS Amt £", 0)
            )
            + ifnull("Forecast PCOS (Std) £", 0)
            + ifnull("Forecast PCOS (Var) £", 0)
        ),
        0
    )
    + ifnull(
        (
            "CY Total Non-Customer Invoiced Fixed Trade £"
            + - ("Forecast Total Non-Customer Invoiced Fixed Trade £")
        ),
        0
    ) as "LW CY GCAT £ (Combo) Act",
    ifnull(
        ifnull(

            ifnull("Cy Invoice Gross Amount £" + "Forecast Gross Amount £", 0)
            + ifnull(("Cy AVP Discount £" * -1) + "Forecast Added Value Pack £", 0)
            + (
                ifnull(
                    "CY Permanent Discounts £"
                    + (-1 * "Forecast Permanent Discounts £"),
                    0
                )
            ),
            0
        ) + ifnull(
            (
                ifnull(
                    "CY Total Customer-Invoiced Trade £"
                    + - ("Forecast Total Customer-Invoiced Trade £"),
                    0
                )
            ),
            0
        ),
        0
    )
    - ifnull((- "CY Total Std PCOS Amt £" + ifnull("Forecast PCOS (Std) £", 0)), 0)
    + ifnull(
        (
            "CY Total Non-Customer Invoiced Fixed Trade £"
            + - ("Forecast Total Non-Customer Invoiced Fixed Trade £")
        ),
        0
    ) as "LW CY GCAT £ (Combo) Std",
    "LW Forecast Net Sales £ (Combo)" + (
        ifnull(
            ifnull(
                "CY Total Customer-Invoiced Trade £"
                + -1 * "Forecast Total Customer-Invoiced Trade £",
                0
            ),
            0
        )
    ) as "LW Forecast NRR £ (Combo)"

from final