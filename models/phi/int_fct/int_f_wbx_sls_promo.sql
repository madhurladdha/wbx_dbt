{{ config(tags=["sales", "promotion", "sls_promo"]) }}

with
    stg_f_wbx_sls_promo as (select * from {{ ref("stg_f_wbx_sls_promo") }}),
    sls_wtx_lkp_snapshot_date as (
        --changed from src_sls_wtx_lkp_snapshot_date to stg_d_wtx_lkp_snapshot_date
        select * from {{ ref("stg_d_wtx_lkp_snapshot_date") }}
    ),
    dim_wbx_item_ext as (select * from {{ ref("dim_wbx_item_ext") }}),
    stage as (
        select
            source_system,
            promo_idx,
            cust_idx,
            plan_source_customer_code,
            sku_idx,
            source_item_identifier,
            to_date(to_char(day_idx), 'YYYYMMDD') as calendar_date,
            trunc(snapshot_date, 'DD') as snapshot_date,
            reportingsku_idx,
            decode(upper(ispromosku), 'FALSE', 'N', 'TRUE', 'Y') as ispromosku,
            decode(upper(iscannibsku), 'FALSE', 'N', 'TRUE', 'Y') as iscannibsku,
            decode(
                upper(issi_prepromoday), 'FALSE', 'N', 'TRUE', 'Y'
            ) as issi_prepromoday,
            decode(
                upper(issi_onpromoday), 'FALSE', 'N', 'TRUE', 'Y'
            ) as issi_onpromoday,
            decode(
                upper(issi_postpromoday), 'FALSE', 'N', 'TRUE', 'Y'
            ) as issi_postpromoday,
            decode(
                upper(isso_prepromoday), 'FALSE', 'N', 'TRUE', 'Y'
            ) as isso_prepromoday,
            decode(
                upper(isso_onpromoday), 'FALSE', 'N', 'TRUE', 'Y'
            ) as isso_onpromoday,
            decode(
                upper(isso_postpromoday), 'FALSE', 'N', 'TRUE', 'Y'
            ) as isso_postpromoday,
            si_b_vol_cse,
            si_b_vol_sgl,
            si_a_vol_cse,
            si_a_vol_sgl,
            si_t_vol_cse,
            si_t_vol_sgl,
            si_m_vol_cse,
            si_m_vol_sgl,
            si_i_vol_cse,
            si_i_vol_sgl,
            so_b_vol_cse,
            so_b_vol_sgl,
            so_a_vol_cse,
            so_a_vol_sgl,
            so_t_vol_cse,
            so_t_vol_sgl,
            so_m_vol_cse,
            so_m_vol_sgl,
            so_i_vol_cse,
            so_i_vol_sgl,
            si_cannib_vol_cse,
            si_cannib_vol_sgl,
            so_cannib_vol_cse,
            so_cannib_vol_sgl,
            si_cannib_basevol_cse,
            si_cannib_basevol_sgl,
            so_cannib_basevol_cse,
            so_cannib_basevol_sgl,
            si_cannib_loss_vol_cse,
            si_cannib_loss_vol_sgl,
            so_cannib_loss_vol_cse,
            so_cannib_loss_vol_sgl,
            si_predip_vol_cse,
            si_predip_vol_sgl,
            si_postdip_vol_cse,
            si_postdip_vol_sgl,
            so_predip_vol_cse,
            so_predip_vol_sgl,
            so_postdip_vol_cse,
            so_postdip_vol_sgl,
            si_predip_basevol_cse,
            si_predip_basevol_sgl,
            si_postdip_basevol_cse,
            si_postdip_basevol_sgl,
            so_predip_basevol_cse,
            so_predip_basevol_sgl,
            so_postdip_basevol_cse,
            so_postdip_basevol_sgl,
            postpromodippercent_si,
            postpromodippercent_so,
            prepromodippercent_si,
            prepromodippercent_so,
            onpromophasingpercent_si,
            onpromophasingpercent_so,
            robfundingrequired,
            a_tot_vol_kg as actuals_tot_vol_kg,
            a_ap_gross_sales_value as actuals_ap_gross_sales_value,
            a_ap_range_support_allowance as actuals_ap_range_support_allowance,
            a_ap_everyday_low_prices as actuals_ap_everyday_low_prices,
            a_ap_permanent_disc as actuals_ap_permanent_disc,
            a_ap_off_invoice_disc as actuals_ap_off_invoice_disc,
            a_ap_invoiced_sales_value as actuals_ap_invoiced_sales_value,
            a_ap_early_settlement_disc as actuals_ap_early_settlement_disc,
            a_ap_growth_incentives as actuals_ap_growth_incentives,
            a_ap_net_sales_value as actuals_ap_net_sales_value,
            a_ap_retro as actuals_ap_retro,
            a_ap_avp_disc as actuals_ap_avp_disc,
            a_ap_variable_trade as actuals_ap_variable_trade,
            a_ap_promo_fixed_funding as actuals_ap_promo_fixed_funding,
            a_ap_range_support_incentives as actuals_ap_range_support_incentives,
            a_ap_net_net_sales_value as actuals_ap_net_net_sales_value,
            a_ap_direct_shopper_marketing as actuals_ap_direct_shopper_marketing,
            a_ap_other_direct_payments as actuals_ap_other_direct_payments,
            a_ap_indirect_shopper_marketing as actuals_ap_indirect_shopper_marketing,
            a_ap_other_indirect_payments as actuals_ap_other_indirect_payments,
            a_ap_fixed_trade_cust_invoiced as actuals_ap_fixed_trade_cust_invoiced,
            a_ap_total_trade_cust_invoiced as actuals_ap_total_trade_cust_invoiced,
            a_ap_fixed_trade_non_cust_invoiced
            as actuals_ap_fixed_trade_non_cust_invoiced,
            a_ap_total_trade as actuals_ap_total_trade,
            a_ap_net_realisable_revenue as actuals_ap_net_realisable_revenue,
            a_ap_tot_prime_cost_standard as actuals_ap_tot_prime_cost_standard,
            a_ap_gross_margin_standard as actuals_ap_gross_margin_standard,
            a_ap_gcat_standard as actuals_ap_gcat_standard,
            a_manso_tot_vol_kg as actuals_manso_tot_vol_kg,
            a_manso_gross_sales_value as actuals_manso_gross_sales_value,
            a_manso_range_support_allowance as actuals_manso_range_support_allowance,
            a_manso_everyday_low_prices as actuals_manso_everyday_low_prices,
            a_manso_permanent_disc as actuals_manso_permanent_disc,
            a_manso_off_invoice_disc as actuals_manso_off_invoice_disc,
            a_manso_invoiced_sales_value as actuals_manso_invoiced_sales_value,
            a_manso_early_settlement_disc as actuals_manso_early_settlement_disc,
            a_manso_growth_incentives as actuals_manso_growth_incentives,
            a_manso_net_sales_value as actuals_manso_net_sales_value,
            a_manso_retro as actuals_manso_retro,
            a_manso_avp_disc as actuals_manso_avp_disc,
            a_manso_variable_trade as actuals_manso_variable_trade,
            a_manso_promo_fixed_funding as actuals_manso_promo_fixed_funding,
            a_manso_range_support_incentives as actuals_manso_range_support_incentives,
            a_manso_net_net_sales_value as actuals_manso_net_net_sales_value,
            a_manso_direct_shopper_marketing as actuals_manso_direct_shopper_marketing,
            a_manso_other_direct_payments as actuals_manso_other_direct_payments,
            a_manso_indirect_shopper_marketing
            as actuals_manso_indirect_shopper_marketing,
            a_manso_other_indirect_payments as actuals_manso_other_indirect_payments,
            a_manso_fixed_trade_cust_invoiced
            as actuals_manso_fixed_trade_cust_invoiced,
            a_manso_total_trade_cust_invoiced
            as actuals_manso_total_trade_cust_invoiced,
            a_manso_fixed_trade_non_cust_invoiced
            as actuals_manso_fixed_trade_non_cust_invoiced,
            a_manso_total_trade as actuals_manso_total_trade,
            a_manso_net_realisable_revenue as actuals_manso_net_realisable_revenue,
            a_manso_tot_prime_cost_standard as actuals_manso_tot_prime_cost_standard,
            a_manso_gross_margin_standard as actuals_manso_gross_margin_standard,
            a_manso_gcat_standard as actuals_manso_gcat_standard,
            a_retail_tot_vol_kg as actuals_retail_tot_vol_kg,
            a_ap_retail_revenue_mrrsp as actuals_ap_retail_revenue_mrrsp,
            a_ap_retail_revenue_rsp as actuals_ap_retail_revenue_rsp,
            a_ap_retail_revenue_net as actuals_ap_retail_revenue_net,
            a_ap_retail_cost_of_sales as actuals_ap_retail_cost_of_sales,
            a_ap_retail_retailer_retro_funding
            as actuals_ap_retail_retailer_retro_funding,
            a_ap_retail_margin_excl_fixed_funding
            as actuals_ap_retail_margin_excl_fixed_funding,
            a_ap_retail_promo_fixed_spend as actuals_ap_retail_promo_fixed_spend,
            a_ap_retail_total_spend as actuals_ap_retail_total_spend,
            a_ap_retail_margin_incl_fixed_funding
            as actuals_ap_retail_margin_incl_fixed_funding,
            a_ap_retail_revenue_net_excl_mrrsp
            as actuals_ap_retail_revenue_net_excl_mrrsp,
            a_ap_retail_revenue_net_excl_rsp as actuals_ap_retail_revenue_net_excl_rsp,
            b_tot_vol_kg as base_tot_vol_kg,
            b_ap_gross_sales_value as base_ap_gross_sales_value,
            b_ap_range_support_allowance as base_ap_range_support_allowance,
            b_ap_everyday_low_prices as base_ap_everyday_low_prices,
            b_ap_permanent_disc as base_ap_permanent_disc,
            b_ap_off_invoice_disc as base_ap_off_invoice_disc,
            b_ap_invoiced_sales_value as base_ap_invoiced_sales_value,
            b_ap_early_settlement_disc as base_ap_early_settlement_disc,
            b_ap_growth_incentives as base_ap_growth_incentives,
            b_ap_net_sales_value as base_ap_net_sales_value,
            b_ap_retro as base_ap_retro,
            b_ap_avp_disc as base_ap_avp_disc,
            b_ap_variable_trade as base_ap_variable_trade,
            b_ap_promo_fixed_funding as base_ap_promo_fixed_funding,
            b_ap_range_support_incentives as base_ap_range_support_incentives,
            b_ap_net_net_sales_value as base_ap_net_net_sales_value,
            b_ap_direct_shopper_marketing as base_ap_direct_shopper_marketing,
            b_ap_other_direct_payments as base_ap_other_direct_payments,
            b_ap_indirect_shopper_marketing as base_ap_indirect_shopper_marketing,
            b_ap_other_indirect_payments as base_ap_other_indirect_payments,
            b_ap_fixed_trade_cust_invoiced as base_ap_fixed_trade_cust_invoiced,
            b_ap_total_trade_cust_invoiced as base_ap_total_trade_cust_invoiced,
            b_ap_fixed_trade_non_cust_invoiced as base_ap_fixed_trade_non_cust_invoiced,
            b_ap_total_trade as base_ap_total_trade,
            b_ap_net_realisable_revenue as base_ap_net_realisable_revenue,
            b_ap_tot_prime_cost_standard as base_ap_tot_prime_cost_standard,
            b_ap_gross_margin_standard as base_ap_gross_margin_standard,
            b_ap_gcat_standard as base_ap_gcat_standard,
            b_manso_tot_vol_kg as base_manso_tot_vol_kg,
            b_manso_gross_sales_value as base_manso_gross_sales_value,
            b_manso_range_support_allowance as base_manso_range_support_allowance,
            b_manso_everyday_low_prices as base_manso_everyday_low_prices,
            b_manso_permanent_disc as base_manso_permanent_disc,
            b_manso_off_invoice_disc as base_manso_off_invoice_disc,
            b_manso_invoiced_sales_value as base_manso_invoiced_sales_value,
            b_manso_early_settlement_disc as base_manso_early_settlement_disc,
            b_manso_growth_incentives as base_manso_growth_incentives,
            b_manso_net_sales_value as base_manso_net_sales_value,
            b_manso_retro as base_manso_retro,
            b_manso_avp_disc as base_manso_avp_disc,
            b_manso_variable_trade as base_manso_variable_trade,
            b_manso_promo_fixed_funding as base_manso_promo_fixed_funding,
            b_manso_range_support_incentives as base_manso_range_support_incentives,
            b_manso_net_net_sales_value as base_manso_net_net_sales_value,
            b_manso_direct_shopper_marketing as base_manso_direct_shopper_marketing,
            b_manso_other_direct_payments as base_manso_other_direct_payments,
            b_manso_indirect_shopper_marketing as base_manso_indirect_shopper_marketing,
            b_manso_other_indirect_payments as base_manso_other_indirect_payments,
            b_manso_fixed_trade_cust_invoiced as base_manso_fixed_trade_cust_invoiced,
            b_manso_total_trade_cust_invoiced as base_manso_total_trade_cust_invoiced,
            b_manso_fixed_trade_non_cust_invoiced
            as base_manso_fixed_trade_non_cust_invoiced,
            b_manso_total_trade as base_manso_total_trade,
            b_manso_net_realisable_revenue as base_manso_net_realisable_revenue,
            b_manso_tot_prime_cost_standard as base_manso_tot_prime_cost_standard,
            b_manso_gross_margin_standard as base_manso_gross_margin_standard,
            b_manso_gcat_standard as base_manso_gcat_standard,
            b_retail_tot_vol_kg as base_retail_tot_vol_kg,
            b_ap_retail_revenue_mrrsp as base_ap_retail_revenue_mrrsp,
            b_ap_retail_revenue_rsp as base_ap_retail_revenue_rsp,
            b_ap_retail_revenue_net as base_ap_retail_revenue_net,
            b_ap_retail_cost_of_sales as base_ap_retail_cost_of_sales,
            b_ap_retail_retailer_retro_funding as base_ap_retail_retailer_retro_funding,
            b_ap_retail_margin_excl_fixed_funding
            as base_ap_retail_margin_excl_fixed_funding,
            b_ap_retail_promo_fixed_spend as base_ap_retail_promo_fixed_spend,
            b_ap_retail_total_spend as base_ap_retail_total_spend,
            b_ap_retail_margin_incl_fixed_funding
            as base_ap_retail_margin_incl_fixed_funding,
            b_ap_retail_revenue_net_excl_mrrsp as base_ap_retail_revenue_net_excl_mrrsp,
            b_ap_retail_revenue_net_excl_rsp as base_ap_retail_revenue_net_excl_rsp,
            t_tot_vol_kg as forecast_tot_vol_kg,
            t_ap_gross_sales_value as forecast_ap_gross_sales_value,
            t_ap_range_support_allowance as forecast_ap_range_support_allowance,
            t_ap_everyday_low_prices as forecast_ap_everyday_low_prices,
            t_ap_permanent_disc as forecast_ap_permanent_disc,
            t_ap_off_invoice_disc as forecast_ap_off_invoice_disc,
            t_ap_invoiced_sales_value as forecast_ap_invoiced_sales_value,
            t_ap_early_settlement_disc as forecast_ap_early_settlement_disc,
            t_ap_growth_incentives as forecast_ap_growth_incentives,
            t_ap_net_sales_value as forecast_ap_net_sales_value,
            t_ap_retro as forecast_ap_retro,
            t_ap_avp_disc as forecast_ap_avp_disc,
            t_ap_variable_trade as forecast_ap_variable_trade,
            t_ap_promo_fixed_funding as forecast_ap_promo_fixed_funding,
            t_ap_range_support_incentives as forecast_ap_range_support_incentives,
            t_ap_net_net_sales_value as forecast_ap_net_net_sales_value,
            t_ap_direct_shopper_marketing as forecast_ap_direct_shopper_marketing,
            t_ap_other_direct_payments as forecast_ap_other_direct_payments,
            t_ap_indirect_shopper_marketing as forecast_ap_indirect_shopper_marketing,
            t_ap_other_indirect_payments as forecast_ap_other_indirect_payments,
            t_ap_fixed_trade_cust_invoiced as forecast_ap_fixed_trade_cust_invoiced,
            t_ap_total_trade_cust_invoiced as forecast_ap_total_trade_cust_invoiced,
            t_ap_fixed_trade_non_cust_invoiced
            as forecast_ap_fixed_trade_non_cust_invoiced,
            t_ap_total_trade as forecast_ap_total_trade,
            t_ap_total_trade_gbp as forecast_ap_total_trade_gbp,
            t_ap_net_realisable_revenue as forecast_ap_net_realisable_revenue,
            t_ap_tot_prime_cost_standard as forecast_ap_tot_prime_cost_standard,
            t_ap_gross_margin_standard as forecast_ap_gross_margin_standard,
            t_ap_gcat_standard as forecast_ap_gcat_standard,
            t_manso_tot_vol_kg as forecast_manso_tot_vol_kg,
            t_manso_gross_sales_value as forecast_manso_gross_sales_value,
            t_manso_range_support_allowance as forecast_manso_range_support_allowance,
            t_manso_everyday_low_prices as forecast_manso_everyday_low_prices,
            t_manso_permanent_disc as forecast_manso_permanent_disc,
            t_manso_off_invoice_disc as forecast_manso_off_invoice_disc,
            t_manso_invoiced_sales_value as forecast_manso_invoiced_sales_value,
            t_manso_early_settlement_disc as forecast_manso_early_settlement_disc,
            t_manso_growth_incentives as forecast_manso_growth_incentives,
            t_manso_net_sales_value as forecast_manso_net_sales_value,
            t_manso_retro as forecast_manso_retro,
            t_manso_avp_disc as forecast_manso_avp_disc,
            t_manso_variable_trade as forecast_manso_variable_trade,
            t_manso_promo_fixed_funding as forecast_manso_promo_fixed_funding,
            t_manso_range_support_incentives as forecast_manso_range_support_incentives,
            t_manso_net_net_sales_value as forecast_manso_net_net_sales_value,
            t_manso_direct_shopper_marketing as forecast_manso_direct_shopper_marketing,
            t_manso_other_direct_payments as forecast_manso_other_direct_payments,
            t_manso_indirect_shopper_marketing
            as forecast_manso_indirect_shopper_marketing,
            t_manso_other_indirect_payments as forecast_manso_other_indirect_payments,
            t_manso_fixed_trade_cust_invoiced
            as forecast_manso_fixed_trade_cust_invoiced,
            t_manso_total_trade_cust_invoiced
            as forecast_manso_total_trade_cust_invoiced,
            t_manso_fixed_trade_non_cust_invoiced
            as forecast_manso_fixed_trade_non_cust_invoiced,
            t_manso_total_trade as forecast_manso_total_trade,
            t_manso_net_realisable_revenue as forecast_manso_net_realisable_revenue,
            t_manso_tot_prime_cost_standard as forecast_manso_tot_prime_cost_standard,
            t_manso_gross_margin_standard as forecast_manso_gross_margin_standard,
            t_manso_gcat_standard as forecast_manso_gcat_standard,
            t_retail_tot_vol_kg as forecast_retail_tot_vol_kg,
            t_ap_retail_revenue_mrrsp as forecast_ap_retail_revenue_mrrsp,
            t_ap_retail_revenue_rsp as forecast_ap_retail_revenue_rsp,
            t_ap_retail_revenue_net as forecast_ap_retail_revenue_net,
            t_ap_retail_cost_of_sales as forecast_ap_retail_cost_of_sales,
            t_ap_retail_retailer_retro_funding
            as forecast_ap_retail_retailer_retro_funding,
            t_ap_retail_margin_excl_fixed_funding
            as forecast_ap_retail_margin_excl_fixed_funding,
            t_ap_retail_promo_fixed_spend as forecast_ap_retail_promo_fixed_spend,
            t_ap_retail_total_spend as forecast_ap_retail_total_spend,
            t_ap_retail_margin_incl_fixed_funding
            as forecast_ap_retail_margin_incl_fixed_funding,
            t_ap_retail_revenue_net_excl_mrrsp
            as forecast_ap_retail_revenue_net_excl_mrrsp,
            t_ap_retail_revenue_net_excl_rsp as forecast_ap_retail_revenue_net_excl_rsp,
            prm_rpt_customer_code,
            promo_code
        from stg_f_wbx_sls_promo
        left join
            (
                select snapshot_date
                from
                    (
                        select *, rank() over (order by snapshot_date desc) rnknum
                        from sls_wtx_lkp_snapshot_date
                    )
                where rnknum = 1
            ) snapshot
            on 1 = 1
        where
            /* -- FILTER ADDED TO ELIMINATE ROWS WHERE COLUMNS HAVING EITHER 0 OR NULL VALUES ONLY -- */
            (
                nvl(si_b_vol_cse, 0) <> 0
                or nvl(si_b_vol_sgl, 0) <> 0
                or nvl(si_a_vol_cse, 0) <> 0
                or nvl(si_a_vol_sgl, 0) <> 0
                or nvl(si_t_vol_cse, 0) <> 0
                or nvl(si_t_vol_sgl, 0) <> 0
                or nvl(si_m_vol_cse, 0) <> 0
                or nvl(si_m_vol_sgl, 0) <> 0
                or nvl(si_i_vol_cse, 0) <> 0
                or nvl(si_i_vol_sgl, 0) <> 0
                or nvl(so_b_vol_cse, 0) <> 0
                or nvl(so_b_vol_sgl, 0) <> 0
                or nvl(so_a_vol_cse, 0) <> 0
                or nvl(so_a_vol_sgl, 0) <> 0
                or nvl(so_t_vol_cse, 0) <> 0
                or nvl(so_t_vol_sgl, 0) <> 0
                or nvl(so_m_vol_cse, 0) <> 0
                or nvl(so_m_vol_sgl, 0) <> 0
                or nvl(so_i_vol_cse, 0) <> 0
                or nvl(so_i_vol_sgl, 0) <> 0
                or nvl(si_cannib_vol_cse, 0) <> 0
                or nvl(si_cannib_vol_sgl, 0) <> 0
                or nvl(so_cannib_vol_cse, 0) <> 0
                or nvl(so_cannib_vol_sgl, 0) <> 0
                or nvl(si_cannib_basevol_cse, 0) <> 0
                or nvl(si_cannib_basevol_sgl, 0) <> 0
                or nvl(so_cannib_basevol_cse, 0) <> 0
                or nvl(so_cannib_basevol_sgl, 0) <> 0
                or nvl(si_cannib_loss_vol_cse, 0) <> 0
                or nvl(si_cannib_loss_vol_sgl, 0) <> 0
                or nvl(so_cannib_loss_vol_cse, 0) <> 0
                or nvl(so_cannib_loss_vol_sgl, 0) <> 0
                or nvl(si_predip_vol_cse, 0) <> 0
                or nvl(si_predip_vol_sgl, 0) <> 0
                or nvl(si_postdip_vol_cse, 0) <> 0
                or nvl(si_postdip_vol_sgl, 0) <> 0
                or nvl(so_predip_vol_cse, 0) <> 0
                or nvl(so_predip_vol_sgl, 0) <> 0
                or nvl(so_postdip_vol_cse, 0) <> 0
                or nvl(so_postdip_vol_sgl, 0) <> 0
                or nvl(si_predip_basevol_cse, 0) <> 0
                or nvl(si_predip_basevol_sgl, 0) <> 0
                or nvl(si_postdip_basevol_cse, 0) <> 0
                or nvl(si_postdip_basevol_sgl, 0) <> 0
                or nvl(so_predip_basevol_cse, 0) <> 0
                or nvl(so_predip_basevol_sgl, 0) <> 0
                or nvl(so_postdip_basevol_cse, 0) <> 0
                or nvl(so_postdip_basevol_sgl, 0) <> 0
                or nvl(postpromodippercent_si, 0) <> 0
                or nvl(postpromodippercent_so, 0) <> 0
                or nvl(prepromodippercent_si, 0) <> 0
                or nvl(prepromodippercent_so, 0) <> 0
                or nvl(onpromophasingpercent_si, 0) <> 0
                or nvl(onpromophasingpercent_so, 0) <> 0
                or nvl(robfundingrequired, 0) <> 0
                or nvl(a_tot_vol_kg, 0) <> 0
                or nvl(a_ap_gross_sales_value, 0) <> 0
                or nvl(a_ap_range_support_allowance, 0) <> 0
                or nvl(a_ap_everyday_low_prices, 0) <> 0
                or nvl(a_ap_permanent_disc, 0) <> 0
                or nvl(a_ap_off_invoice_disc, 0) <> 0
                or nvl(a_ap_invoiced_sales_value, 0) <> 0
                or nvl(a_ap_early_settlement_disc, 0) <> 0
                or nvl(a_ap_growth_incentives, 0) <> 0
                or nvl(a_ap_net_sales_value, 0) <> 0
                or nvl(a_ap_retro, 0) <> 0
                or nvl(a_ap_avp_disc, 0) <> 0
                or nvl(a_ap_variable_trade, 0) <> 0
                or nvl(a_ap_promo_fixed_funding, 0) <> 0
                or nvl(a_ap_range_support_incentives, 0) <> 0
                or nvl(a_ap_net_net_sales_value, 0) <> 0
                or nvl(a_ap_direct_shopper_marketing, 0) <> 0
                or nvl(a_ap_other_direct_payments, 0) <> 0
                or nvl(a_ap_indirect_shopper_marketing, 0) <> 0
                or nvl(a_ap_other_indirect_payments, 0) <> 0
                or nvl(a_ap_fixed_trade_cust_invoiced, 0) <> 0
                or nvl(a_ap_total_trade_cust_invoiced, 0) <> 0
                or nvl(a_ap_fixed_trade_non_cust_invoiced, 0) <> 0
                or nvl(a_ap_total_trade, 0) <> 0
                or nvl(a_ap_net_realisable_revenue, 0) <> 0
                or nvl(a_ap_tot_prime_cost_standard, 0) <> 0
                or nvl(a_ap_gross_margin_standard, 0) <> 0
                or nvl(a_ap_gcat_standard, 0) <> 0
                or nvl(a_manso_tot_vol_kg, 0) <> 0
                or nvl(a_manso_gross_sales_value, 0) <> 0
                or nvl(a_manso_range_support_allowance, 0) <> 0
                or nvl(a_manso_everyday_low_prices, 0) <> 0
                or nvl(a_manso_permanent_disc, 0) <> 0
                or nvl(a_manso_off_invoice_disc, 0) <> 0
                or nvl(a_manso_invoiced_sales_value, 0) <> 0
                or nvl(a_manso_early_settlement_disc, 0) <> 0
                or nvl(a_manso_growth_incentives, 0) <> 0
                or nvl(a_manso_net_sales_value, 0) <> 0
                or nvl(a_manso_retro, 0) <> 0
                or nvl(a_manso_avp_disc, 0) <> 0
                or nvl(a_manso_variable_trade, 0) <> 0
                or nvl(a_manso_promo_fixed_funding, 0) <> 0
                or nvl(a_manso_range_support_incentives, 0) <> 0
                or nvl(a_manso_net_net_sales_value, 0) <> 0
                or nvl(a_manso_direct_shopper_marketing, 0) <> 0
                or nvl(a_manso_other_direct_payments, 0) <> 0
                or nvl(a_manso_indirect_shopper_marketing, 0) <> 0
                or nvl(a_manso_other_indirect_payments, 0) <> 0
                or nvl(a_manso_fixed_trade_cust_invoiced, 0) <> 0
                or nvl(a_manso_total_trade_cust_invoiced, 0) <> 0
                or nvl(a_manso_fixed_trade_non_cust_invoiced, 0) <> 0
                or nvl(a_manso_total_trade, 0) <> 0
                or nvl(a_manso_net_realisable_revenue, 0) <> 0
                or nvl(a_manso_tot_prime_cost_standard, 0) <> 0
                or nvl(a_manso_gross_margin_standard, 0) <> 0
                or nvl(a_manso_gcat_standard, 0) <> 0
                or nvl(a_retail_tot_vol_kg, 0) <> 0
                or nvl(a_ap_retail_revenue_mrrsp, 0) <> 0
                or nvl(a_ap_retail_revenue_rsp, 0) <> 0
                or nvl(a_ap_retail_revenue_net, 0) <> 0
                or nvl(a_ap_retail_cost_of_sales, 0) <> 0
                or nvl(a_ap_retail_retailer_retro_funding, 0) <> 0
                or nvl(a_ap_retail_margin_excl_fixed_funding, 0) <> 0
                or nvl(a_ap_retail_promo_fixed_spend, 0) <> 0
                or nvl(a_ap_retail_total_spend, 0) <> 0
                or nvl(a_ap_retail_margin_incl_fixed_funding, 0) <> 0
                or nvl(a_ap_retail_revenue_net_excl_mrrsp, 0) <> 0
                or nvl(a_ap_retail_revenue_net_excl_rsp, 0) <> 0
                or nvl(b_tot_vol_kg, 0) <> 0
                or nvl(b_ap_gross_sales_value, 0) <> 0
                or nvl(b_ap_range_support_allowance, 0) <> 0
                or nvl(b_ap_everyday_low_prices, 0) <> 0
                or nvl(b_ap_permanent_disc, 0) <> 0
                or nvl(b_ap_off_invoice_disc, 0) <> 0
                or nvl(b_ap_invoiced_sales_value, 0) <> 0
                or nvl(b_ap_early_settlement_disc, 0) <> 0
                or nvl(b_ap_growth_incentives, 0) <> 0
                or nvl(b_ap_net_sales_value, 0) <> 0
                or nvl(b_ap_retro, 0) <> 0
                or nvl(b_ap_avp_disc, 0) <> 0
                or nvl(b_ap_variable_trade, 0) <> 0
                or nvl(b_ap_promo_fixed_funding, 0) <> 0
                or nvl(b_ap_range_support_incentives, 0) <> 0
                or nvl(b_ap_net_net_sales_value, 0) <> 0
                or nvl(b_ap_direct_shopper_marketing, 0) <> 0
                or nvl(b_ap_other_direct_payments, 0) <> 0
                or nvl(b_ap_indirect_shopper_marketing, 0) <> 0
                or nvl(b_ap_other_indirect_payments, 0) <> 0
                or nvl(b_ap_fixed_trade_cust_invoiced, 0) <> 0
                or nvl(b_ap_total_trade_cust_invoiced, 0) <> 0
                or nvl(b_ap_fixed_trade_non_cust_invoiced, 0) <> 0
                or nvl(b_ap_total_trade, 0) <> 0
                or nvl(b_ap_net_realisable_revenue, 0) <> 0
                or nvl(b_ap_tot_prime_cost_standard, 0) <> 0
                or nvl(b_ap_gross_margin_standard, 0) <> 0
                or nvl(b_ap_gcat_standard, 0) <> 0
                or nvl(b_manso_tot_vol_kg, 0) <> 0
                or nvl(b_manso_gross_sales_value, 0) <> 0
                or nvl(b_manso_range_support_allowance, 0) <> 0
                or nvl(b_manso_everyday_low_prices, 0) <> 0
                or nvl(b_manso_permanent_disc, 0) <> 0
                or nvl(b_manso_off_invoice_disc, 0) <> 0
                or nvl(b_manso_invoiced_sales_value, 0) <> 0
                or nvl(b_manso_early_settlement_disc, 0) <> 0
                or nvl(b_manso_growth_incentives, 0) <> 0
                or nvl(b_manso_net_sales_value, 0) <> 0
                or nvl(b_manso_retro, 0) <> 0
                or nvl(b_manso_avp_disc, 0) <> 0
                or nvl(b_manso_variable_trade, 0) <> 0
                or nvl(b_manso_promo_fixed_funding, 0) <> 0
                or nvl(b_manso_range_support_incentives, 0) <> 0
                or nvl(b_manso_net_net_sales_value, 0) <> 0
                or nvl(b_manso_direct_shopper_marketing, 0) <> 0
                or nvl(b_manso_other_direct_payments, 0) <> 0
                or nvl(b_manso_indirect_shopper_marketing, 0) <> 0
                or nvl(b_manso_other_indirect_payments, 0) <> 0
                or nvl(b_manso_fixed_trade_cust_invoiced, 0) <> 0
                or nvl(b_manso_total_trade_cust_invoiced, 0) <> 0
                or nvl(b_manso_fixed_trade_non_cust_invoiced, 0) <> 0
                or nvl(b_manso_total_trade, 0) <> 0
                or nvl(b_manso_net_realisable_revenue, 0) <> 0
                or nvl(b_manso_tot_prime_cost_standard, 0) <> 0
                or nvl(b_manso_gross_margin_standard, 0) <> 0
                or nvl(b_manso_gcat_standard, 0) <> 0
                or nvl(b_retail_tot_vol_kg, 0) <> 0
                or nvl(b_ap_retail_revenue_mrrsp, 0) <> 0
                or nvl(b_ap_retail_revenue_rsp, 0) <> 0
                or nvl(b_ap_retail_revenue_net, 0) <> 0
                or nvl(b_ap_retail_cost_of_sales, 0) <> 0
                or nvl(b_ap_retail_retailer_retro_funding, 0) <> 0
                or nvl(b_ap_retail_margin_excl_fixed_funding, 0) <> 0
                or nvl(b_ap_retail_promo_fixed_spend, 0) <> 0
                or nvl(b_ap_retail_total_spend, 0) <> 0
                or nvl(b_ap_retail_margin_incl_fixed_funding, 0) <> 0
                or nvl(b_ap_retail_revenue_net_excl_mrrsp, 0) <> 0
                or nvl(b_ap_retail_revenue_net_excl_rsp, 0) <> 0
                or nvl(t_tot_vol_kg, 0) <> 0
                or nvl(t_ap_gross_sales_value, 0) <> 0
                or nvl(t_ap_range_support_allowance, 0) <> 0
                or nvl(t_ap_everyday_low_prices, 0) <> 0
                or nvl(t_ap_permanent_disc, 0) <> 0
                or nvl(t_ap_off_invoice_disc, 0) <> 0
                or nvl(t_ap_invoiced_sales_value, 0) <> 0
                or nvl(t_ap_early_settlement_disc, 0) <> 0
                or nvl(t_ap_growth_incentives, 0) <> 0
                or nvl(t_ap_net_sales_value, 0) <> 0
                or nvl(t_ap_retro, 0) <> 0
                or nvl(t_ap_avp_disc, 0) <> 0
                or nvl(t_ap_variable_trade, 0) <> 0
                or nvl(t_ap_promo_fixed_funding, 0) <> 0
                or nvl(t_ap_range_support_incentives, 0) <> 0
                or nvl(t_ap_net_net_sales_value, 0) <> 0
                or nvl(t_ap_direct_shopper_marketing, 0) <> 0
                or nvl(t_ap_other_direct_payments, 0) <> 0
                or nvl(t_ap_indirect_shopper_marketing, 0) <> 0
                or nvl(t_ap_other_indirect_payments, 0) <> 0
                or nvl(t_ap_fixed_trade_cust_invoiced, 0) <> 0
                or nvl(t_ap_total_trade_cust_invoiced, 0) <> 0
                or nvl(t_ap_fixed_trade_non_cust_invoiced, 0) <> 0
                or nvl(t_ap_total_trade, 0) <> 0
                or nvl(t_ap_total_trade_gbp, 0) <> 0
                or nvl(t_ap_net_realisable_revenue, 0) <> 0
                or nvl(t_ap_tot_prime_cost_standard, 0) <> 0
                or nvl(t_ap_gross_margin_standard, 0) <> 0
                or nvl(t_ap_gcat_standard, 0) <> 0
                or nvl(t_manso_tot_vol_kg, 0) <> 0
                or nvl(t_manso_gross_sales_value, 0) <> 0
                or nvl(t_manso_range_support_allowance, 0) <> 0
                or nvl(t_manso_everyday_low_prices, 0) <> 0
                or nvl(t_manso_permanent_disc, 0) <> 0
                or nvl(t_manso_off_invoice_disc, 0) <> 0
                or nvl(t_manso_invoiced_sales_value, 0) <> 0
                or nvl(t_manso_early_settlement_disc, 0) <> 0
                or nvl(t_manso_growth_incentives, 0) <> 0
                or nvl(t_manso_net_sales_value, 0) <> 0
                or nvl(t_manso_retro, 0) <> 0
                or nvl(t_manso_avp_disc, 0) <> 0
                or nvl(t_manso_variable_trade, 0) <> 0
                or nvl(t_manso_promo_fixed_funding, 0) <> 0
                or nvl(t_manso_range_support_incentives, 0) <> 0
                or nvl(t_manso_net_net_sales_value, 0) <> 0
                or nvl(t_manso_direct_shopper_marketing, 0) <> 0
                or nvl(t_manso_other_direct_payments, 0) <> 0
                or nvl(t_manso_indirect_shopper_marketing, 0) <> 0
                or nvl(t_manso_other_indirect_payments, 0) <> 0
                or nvl(t_manso_fixed_trade_cust_invoiced, 0) <> 0
                or nvl(t_manso_total_trade_cust_invoiced, 0) <> 0
                or nvl(t_manso_fixed_trade_non_cust_invoiced, 0) <> 0
                or nvl(t_manso_total_trade, 0) <> 0
                or nvl(t_manso_net_realisable_revenue, 0) <> 0
                or nvl(t_manso_tot_prime_cost_standard, 0) <> 0
                or nvl(t_manso_gross_margin_standard, 0) <> 0
                or nvl(t_manso_gcat_standard, 0) <> 0
                or nvl(t_retail_tot_vol_kg, 0) <> 0
                or nvl(t_ap_retail_revenue_mrrsp, 0) <> 0
                or nvl(t_ap_retail_revenue_rsp, 0) <> 0
                or nvl(t_ap_retail_revenue_net, 0) <> 0
                or nvl(t_ap_retail_cost_of_sales, 0) <> 0
                or nvl(t_ap_retail_retailer_retro_funding, 0) <> 0
                or nvl(t_ap_retail_margin_excl_fixed_funding, 0) <> 0
                or nvl(t_ap_retail_promo_fixed_spend, 0) <> 0
                or nvl(t_ap_retail_total_spend, 0) <> 0
                or nvl(t_ap_retail_margin_incl_fixed_funding, 0) <> 0
                or nvl(t_ap_retail_revenue_net_excl_mrrsp, 0) <> 0
                or nvl(t_ap_retail_revenue_net_excl_rsp, 0) <> 0
            )
    ),
    item_master as (
        select distinct source_system, item_guid, source_item_identifier
        from dim_wbx_item_ext
        where source_system = 'WEETABIX'
    ),
    final as (
        select
            stg.source_system,
            promo_idx,
            -- promo_lkp.promo_guid as promo_guid,
            {{
                dbt_utils.surrogate_key(
                    ["stg.source_system", "stg.promo_idx"]
                )
            }} as promo_guid,
            cust_idx,
            plan_source_customer_code,
            0 as customer_address_number_guid,
            sku_idx,
            stg.source_item_identifier,
            -- item_lkp.item_guid as item_guid,
            {{
                dbt_utils.surrogate_key(
                    ["stg.source_system", "stg.source_item_identifier"]
                )
            }} as item_guid,
            calendar_date,
            stg.snapshot_date,
            reportingsku_idx,
            ispromosku,
            iscannibsku,
            issi_prepromoday,
            issi_onpromoday,
            issi_postpromoday,
            isso_prepromoday,
            isso_onpromoday,
            isso_postpromoday,
            si_b_vol_cse,
            si_b_vol_sgl,
            si_a_vol_cse,
            si_a_vol_sgl,
            si_t_vol_cse,
            si_t_vol_sgl,
            si_m_vol_cse,
            si_m_vol_sgl,
            si_i_vol_cse,
            si_i_vol_sgl,
            so_b_vol_cse,
            so_b_vol_sgl,
            so_a_vol_cse,
            so_a_vol_sgl,
            so_t_vol_cse,
            so_t_vol_sgl,
            so_m_vol_cse,
            so_m_vol_sgl,
            so_i_vol_cse,
            so_i_vol_sgl,
            si_cannib_vol_cse,
            si_cannib_vol_sgl,
            so_cannib_vol_cse,
            so_cannib_vol_sgl,
            si_cannib_basevol_cse,
            si_cannib_basevol_sgl,
            so_cannib_basevol_cse,
            so_cannib_basevol_sgl,
            si_cannib_loss_vol_cse,
            si_cannib_loss_vol_sgl,
            so_cannib_loss_vol_cse,
            so_cannib_loss_vol_sgl,
            si_predip_vol_cse,
            si_predip_vol_sgl,
            si_postdip_vol_cse,
            si_postdip_vol_sgl,
            so_predip_vol_cse,
            so_predip_vol_sgl,
            so_postdip_vol_cse,
            so_postdip_vol_sgl,
            si_predip_basevol_cse,
            si_predip_basevol_sgl,
            si_postdip_basevol_cse,
            si_postdip_basevol_sgl,
            so_predip_basevol_cse,
            so_predip_basevol_sgl,
            so_postdip_basevol_cse,
            so_postdip_basevol_sgl,
            postpromodippercent_si,
            postpromodippercent_so,
            prepromodippercent_si,
            prepromodippercent_so,
            onpromophasingpercent_si,
            onpromophasingpercent_so,
            robfundingrequired,
            actuals_tot_vol_kg,
            actuals_ap_gross_sales_value,
            actuals_ap_range_support_allowance,
            actuals_ap_everyday_low_prices,
            actuals_ap_permanent_disc,
            actuals_ap_off_invoice_disc,
            actuals_ap_invoiced_sales_value,
            actuals_ap_early_settlement_disc,
            actuals_ap_growth_incentives,
            actuals_ap_net_sales_value,
            actuals_ap_retro,
            actuals_ap_avp_disc,
            actuals_ap_variable_trade,
            actuals_ap_promo_fixed_funding,
            actuals_ap_range_support_incentives,
            actuals_ap_net_net_sales_value,
            actuals_ap_direct_shopper_marketing,
            actuals_ap_other_direct_payments,
            actuals_ap_indirect_shopper_marketing,
            actuals_ap_other_indirect_payments,
            actuals_ap_fixed_trade_cust_invoiced,
            actuals_ap_total_trade_cust_invoiced,
            actuals_ap_fixed_trade_non_cust_invoiced,
            actuals_ap_total_trade,
            actuals_ap_net_realisable_revenue,
            actuals_ap_tot_prime_cost_standard,
            actuals_ap_gross_margin_standard,
            actuals_ap_gcat_standard,
            actuals_manso_tot_vol_kg,
            actuals_manso_gross_sales_value,
            actuals_manso_range_support_allowance,
            actuals_manso_everyday_low_prices,
            actuals_manso_permanent_disc,
            actuals_manso_off_invoice_disc,
            actuals_manso_invoiced_sales_value,
            actuals_manso_early_settlement_disc,
            actuals_manso_growth_incentives,
            actuals_manso_net_sales_value,
            actuals_manso_retro,
            actuals_manso_avp_disc,
            actuals_manso_variable_trade,
            actuals_manso_promo_fixed_funding,
            actuals_manso_range_support_incentives,
            actuals_manso_net_net_sales_value,
            actuals_manso_direct_shopper_marketing,
            actuals_manso_other_direct_payments,
            actuals_manso_indirect_shopper_marketing,
            actuals_manso_other_indirect_payments,
            actuals_manso_fixed_trade_cust_invoiced,
            actuals_manso_total_trade_cust_invoiced,
            actuals_manso_fixed_trade_non_cust_invoiced,
            actuals_manso_total_trade,
            actuals_manso_net_realisable_revenue,
            actuals_manso_tot_prime_cost_standard,
            actuals_manso_gross_margin_standard,
            actuals_manso_gcat_standard,
            actuals_retail_tot_vol_kg,
            actuals_ap_retail_revenue_mrrsp,
            actuals_ap_retail_revenue_rsp,
            actuals_ap_retail_revenue_net,
            actuals_ap_retail_cost_of_sales,
            actuals_ap_retail_retailer_retro_funding,
            actuals_ap_retail_margin_excl_fixed_funding,
            actuals_ap_retail_promo_fixed_spend,
            actuals_ap_retail_total_spend,
            actuals_ap_retail_margin_incl_fixed_funding,
            actuals_ap_retail_revenue_net_excl_mrrsp,
            actuals_ap_retail_revenue_net_excl_rsp,
            base_tot_vol_kg,
            base_ap_gross_sales_value,
            base_ap_range_support_allowance,
            base_ap_everyday_low_prices,
            base_ap_permanent_disc,
            base_ap_off_invoice_disc,
            base_ap_invoiced_sales_value,
            base_ap_early_settlement_disc,
            base_ap_growth_incentives,
            base_ap_net_sales_value,
            base_ap_retro,
            base_ap_avp_disc,
            base_ap_variable_trade,
            base_ap_promo_fixed_funding,
            base_ap_range_support_incentives,
            base_ap_net_net_sales_value,
            base_ap_direct_shopper_marketing,
            base_ap_other_direct_payments,
            base_ap_indirect_shopper_marketing,
            base_ap_other_indirect_payments,
            base_ap_fixed_trade_cust_invoiced,
            base_ap_total_trade_cust_invoiced,
            base_ap_fixed_trade_non_cust_invoiced,
            base_ap_total_trade,
            base_ap_net_realisable_revenue,
            base_ap_tot_prime_cost_standard,
            base_ap_gross_margin_standard,
            base_ap_gcat_standard,
            base_manso_tot_vol_kg,
            base_manso_gross_sales_value,
            base_manso_range_support_allowance,
            base_manso_everyday_low_prices,
            base_manso_permanent_disc,
            base_manso_off_invoice_disc,
            base_manso_invoiced_sales_value,
            base_manso_early_settlement_disc,
            base_manso_growth_incentives,
            base_manso_net_sales_value,
            base_manso_retro,
            base_manso_avp_disc,
            base_manso_variable_trade,
            base_manso_promo_fixed_funding,
            base_manso_range_support_incentives,
            base_manso_net_net_sales_value,
            base_manso_direct_shopper_marketing,
            base_manso_other_direct_payments,
            base_manso_indirect_shopper_marketing,
            base_manso_other_indirect_payments,
            base_manso_fixed_trade_cust_invoiced,
            base_manso_total_trade_cust_invoiced,
            base_manso_fixed_trade_non_cust_invoiced,
            base_manso_total_trade,
            base_manso_net_realisable_revenue,
            base_manso_tot_prime_cost_standard,
            base_manso_gross_margin_standard,
            base_manso_gcat_standard,
            base_retail_tot_vol_kg,
            base_ap_retail_revenue_mrrsp,
            base_ap_retail_revenue_rsp,
            base_ap_retail_revenue_net,
            base_ap_retail_cost_of_sales,
            base_ap_retail_retailer_retro_funding,
            base_ap_retail_margin_excl_fixed_funding,
            base_ap_retail_promo_fixed_spend,
            base_ap_retail_total_spend,
            base_ap_retail_margin_incl_fixed_funding,
            base_ap_retail_revenue_net_excl_mrrsp,
            base_ap_retail_revenue_net_excl_rsp,
            forecast_tot_vol_kg,
            forecast_ap_gross_sales_value,
            forecast_ap_range_support_allowance,
            forecast_ap_everyday_low_prices,
            forecast_ap_permanent_disc,
            forecast_ap_off_invoice_disc,
            forecast_ap_invoiced_sales_value,
            forecast_ap_early_settlement_disc,
            forecast_ap_growth_incentives,
            forecast_ap_net_sales_value,
            forecast_ap_retro,
            forecast_ap_avp_disc,
            forecast_ap_variable_trade,
            forecast_ap_promo_fixed_funding,
            forecast_ap_range_support_incentives,
            forecast_ap_net_net_sales_value,
            forecast_ap_direct_shopper_marketing,
            forecast_ap_other_direct_payments,
            forecast_ap_indirect_shopper_marketing,
            forecast_ap_other_indirect_payments,
            forecast_ap_fixed_trade_cust_invoiced,
            forecast_ap_total_trade_cust_invoiced,
            forecast_ap_fixed_trade_non_cust_invoiced,
            forecast_ap_total_trade,
            forecast_ap_total_trade_gbp,
            forecast_ap_net_realisable_revenue,
            forecast_ap_tot_prime_cost_standard,
            forecast_ap_gross_margin_standard,
            forecast_ap_gcat_standard,
            forecast_manso_tot_vol_kg,
            forecast_manso_gross_sales_value,
            forecast_manso_range_support_allowance,
            forecast_manso_everyday_low_prices,
            forecast_manso_permanent_disc,
            forecast_manso_off_invoice_disc,
            forecast_manso_invoiced_sales_value,
            forecast_manso_early_settlement_disc,
            forecast_manso_growth_incentives,
            forecast_manso_net_sales_value,
            forecast_manso_retro,
            forecast_manso_avp_disc,
            forecast_manso_variable_trade,
            forecast_manso_promo_fixed_funding,
            forecast_manso_range_support_incentives,
            forecast_manso_net_net_sales_value,
            forecast_manso_direct_shopper_marketing,
            forecast_manso_other_direct_payments,
            forecast_manso_indirect_shopper_marketing,
            forecast_manso_other_indirect_payments,
            forecast_manso_fixed_trade_cust_invoiced,
            forecast_manso_total_trade_cust_invoiced,
            forecast_manso_fixed_trade_non_cust_invoiced,
            forecast_manso_total_trade,
            forecast_manso_net_realisable_revenue,
            forecast_manso_tot_prime_cost_standard,
            forecast_manso_gross_margin_standard,
            forecast_manso_gcat_standard,
            forecast_retail_tot_vol_kg,
            forecast_ap_retail_revenue_mrrsp,
            forecast_ap_retail_revenue_rsp,
            forecast_ap_retail_revenue_net,
            forecast_ap_retail_cost_of_sales,
            forecast_ap_retail_retailer_retro_funding,
            forecast_ap_retail_margin_excl_fixed_funding,
            forecast_ap_retail_promo_fixed_spend,
            forecast_ap_retail_total_spend,
            forecast_ap_retail_margin_incl_fixed_funding,
            forecast_ap_retail_revenue_net_excl_mrrsp,
            forecast_ap_retail_revenue_net_excl_rsp,
            nvl(uom_ca_kg_lkp.conversion_rate, 0) as v_ca_kg_conv,
            si_b_vol_cse * v_ca_kg_conv as si_b_vol_kg,
            si_a_vol_cse * v_ca_kg_conv as si_a_vol_kg,
            si_t_vol_cse * v_ca_kg_conv as si_t_vol_kg,
            si_m_vol_cse * v_ca_kg_conv as si_m_vol_kg,
            si_i_vol_cse * v_ca_kg_conv as si_i_vol_kg,
            so_b_vol_cse * v_ca_kg_conv as so_b_vol_kg,
            so_a_vol_cse * v_ca_kg_conv as so_a_vol_kg,
            so_t_vol_cse * v_ca_kg_conv as so_t_vol_kg,
            so_m_vol_cse * v_ca_kg_conv as so_m_vol_kg,
            so_i_vol_cse * v_ca_kg_conv as so_i_vol_kg,
            si_cannib_vol_cse * v_ca_kg_conv as si_cannib_vol_kg,
            so_cannib_vol_cse * v_ca_kg_conv as so_cannib_vol_kg,
            si_cannib_basevol_cse * v_ca_kg_conv as si_cannib_basevol_kg,
            so_cannib_basevol_cse * v_ca_kg_conv as so_cannib_basevol_kg,
            si_cannib_loss_vol_cse * v_ca_kg_conv as si_cannib_loss_vol_kg,
            so_cannib_loss_vol_cse * v_ca_kg_conv as so_cannib_loss_vol_kg,
            si_predip_vol_cse * v_ca_kg_conv as si_predip_vol_kg,
            si_postdip_vol_cse * v_ca_kg_conv as si_postdip_vol_kg,
            so_predip_vol_cse * v_ca_kg_conv as so_predip_vol_kg,
            so_postdip_vol_cse * v_ca_kg_conv as so_postdip_vol_kg,
            si_predip_basevol_cse * v_ca_kg_conv as si_predip_basevol_kg,
            si_postdip_basevol_cse * v_ca_kg_conv as si_postdip_basevol_kg,
            so_predip_basevol_cse * v_ca_kg_conv as so_predip_basevol_kg,
            so_postdip_basevol_cse * v_ca_kg_conv as so_postdip_basevol_kg,
            uom_kg_ca_lkp.conversion_rate as v_kg_ca_conv,
            uom_ca_pl_lkp.conversion_rate as v_ca_pl_conv,
            actuals_tot_vol_kg * v_kg_ca_conv as actuals_tot_vol_ca,
            actuals_tot_vol_ca * v_ca_pl_conv as actuals_tot_vol_ul,
            base_tot_vol_kg * v_kg_ca_conv as base_tot_vol_ca,
            base_tot_vol_ca * v_ca_pl_conv as base_tot_vol_ul,
            forecast_tot_vol_kg * v_kg_ca_conv as forecast_tot_vol_ca,
            forecast_tot_vol_ca * v_ca_pl_conv as forecast_tot_vol_ul,
            prm_rpt_customer_code,
            -- customer_lkp.customer_address_number_guid as prm_rpt_customer_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "stg.source_system",
                        "stg.prm_rpt_customer_code",
                        "'CUSTOMER_MAIN'","'WBX'"
                    ]
                )
            }} as prm_rpt_customer_guid,
            promo_code

        from stage stg
        join
            item_master item_lkp
            on stg.source_system = item_lkp.source_system
            and stg.source_item_identifier = item_lkp.source_item_identifier
        left join
            {{
                ent_dbt_package.lkp_uom(
                    "item_lkp.item_guid",
                    "'KG'",
                    "'CA'",
                    "uom_kg_ca_lkp",
                )
            }}
        left join
            {{
                ent_dbt_package.lkp_uom(
                    "item_lkp.item_guid",
                    "'CA'",
                    "'KG'",
                    "uom_ca_kg_lkp",
                )
            }}
        left join
            {{
                ent_dbt_package.lkp_uom(
                    "item_lkp.item_guid",
                    "'CA'",
                    "'PL'",
                    "uom_ca_pl_lkp",
                )
            }}
    )

select
    source_system,
    promo_idx,
    promo_guid,
    cust_idx,
    plan_source_customer_code,
    customer_address_number_guid,
    sku_idx,
    source_item_identifier,
    item_guid,
    calendar_date,
    snapshot_date,
    reportingsku_idx,
    ispromosku,
    iscannibsku,
    issi_prepromoday,
    issi_onpromoday,
    issi_postpromoday,
    isso_prepromoday,
    isso_onpromoday,
    isso_postpromoday,
    si_b_vol_cse,
    si_b_vol_sgl,
    si_a_vol_cse,
    si_a_vol_sgl,
    si_t_vol_cse,
    si_t_vol_sgl,
    si_m_vol_cse,
    si_m_vol_sgl,
    si_i_vol_cse,
    si_i_vol_sgl,
    so_b_vol_cse,
    so_b_vol_sgl,
    so_a_vol_cse,
    so_a_vol_sgl,
    so_t_vol_cse,
    so_t_vol_sgl,
    so_m_vol_cse,
    so_m_vol_sgl,
    so_i_vol_cse,
    so_i_vol_sgl,
    si_cannib_vol_cse,
    si_cannib_vol_sgl,
    so_cannib_vol_cse,
    so_cannib_vol_sgl,
    si_cannib_basevol_cse,
    si_cannib_basevol_sgl,
    so_cannib_basevol_cse,
    so_cannib_basevol_sgl,
    si_cannib_loss_vol_cse,
    si_cannib_loss_vol_sgl,
    so_cannib_loss_vol_cse,
    so_cannib_loss_vol_sgl,
    si_predip_vol_cse,
    si_predip_vol_sgl,
    si_postdip_vol_cse,
    si_postdip_vol_sgl,
    so_predip_vol_cse,
    so_predip_vol_sgl,
    so_postdip_vol_cse,
    so_postdip_vol_sgl,
    si_predip_basevol_cse,
    si_predip_basevol_sgl,
    si_postdip_basevol_cse,
    si_postdip_basevol_sgl,
    so_predip_basevol_cse,
    so_predip_basevol_sgl,
    so_postdip_basevol_cse,
    so_postdip_basevol_sgl,
    postpromodippercent_si,
    postpromodippercent_so,
    prepromodippercent_si,
    prepromodippercent_so,
    onpromophasingpercent_si,
    onpromophasingpercent_so,
    robfundingrequired,
    actuals_tot_vol_kg,
    actuals_ap_gross_sales_value,
    actuals_ap_range_support_allowance,
    actuals_ap_everyday_low_prices,
    actuals_ap_permanent_disc,
    actuals_ap_off_invoice_disc,
    actuals_ap_invoiced_sales_value,
    actuals_ap_early_settlement_disc,
    actuals_ap_growth_incentives,
    actuals_ap_net_sales_value,
    actuals_ap_retro,
    actuals_ap_avp_disc,
    actuals_ap_variable_trade,
    actuals_ap_promo_fixed_funding,
    actuals_ap_range_support_incentives,
    actuals_ap_net_net_sales_value,
    actuals_ap_direct_shopper_marketing,
    actuals_ap_other_direct_payments,
    actuals_ap_indirect_shopper_marketing,
    actuals_ap_other_indirect_payments,
    actuals_ap_fixed_trade_cust_invoiced,
    actuals_ap_total_trade_cust_invoiced,
    actuals_ap_fixed_trade_non_cust_invoiced,
    actuals_ap_total_trade,
    actuals_ap_net_realisable_revenue,
    actuals_ap_tot_prime_cost_standard,
    actuals_ap_gross_margin_standard,
    actuals_ap_gcat_standard,
    actuals_manso_tot_vol_kg,
    actuals_manso_gross_sales_value,
    actuals_manso_range_support_allowance,
    actuals_manso_everyday_low_prices,
    actuals_manso_permanent_disc,
    actuals_manso_off_invoice_disc,
    actuals_manso_invoiced_sales_value,
    actuals_manso_early_settlement_disc,
    actuals_manso_growth_incentives,
    actuals_manso_net_sales_value,
    actuals_manso_retro,
    actuals_manso_avp_disc,
    actuals_manso_variable_trade,
    actuals_manso_promo_fixed_funding,
    actuals_manso_range_support_incentives,
    actuals_manso_net_net_sales_value,
    actuals_manso_direct_shopper_marketing,
    actuals_manso_other_direct_payments,
    actuals_manso_indirect_shopper_marketing,
    actuals_manso_other_indirect_payments,
    actuals_manso_fixed_trade_cust_invoiced,
    actuals_manso_total_trade_cust_invoiced,
    actuals_manso_fixed_trade_non_cust_invoiced,
    actuals_manso_total_trade,
    actuals_manso_net_realisable_revenue,
    actuals_manso_tot_prime_cost_standard,
    actuals_manso_gross_margin_standard,
    actuals_manso_gcat_standard,
    actuals_retail_tot_vol_kg,
    actuals_ap_retail_revenue_mrrsp,
    actuals_ap_retail_revenue_rsp,
    actuals_ap_retail_revenue_net,
    actuals_ap_retail_cost_of_sales,
    actuals_ap_retail_retailer_retro_funding,
    actuals_ap_retail_margin_excl_fixed_funding,
    actuals_ap_retail_promo_fixed_spend,
    actuals_ap_retail_total_spend,
    actuals_ap_retail_margin_incl_fixed_funding,
    actuals_ap_retail_revenue_net_excl_mrrsp,
    actuals_ap_retail_revenue_net_excl_rsp,
    base_tot_vol_kg,
    base_ap_gross_sales_value,
    base_ap_range_support_allowance,
    base_ap_everyday_low_prices,
    base_ap_permanent_disc,
    base_ap_off_invoice_disc,
    base_ap_invoiced_sales_value,
    base_ap_early_settlement_disc,
    base_ap_growth_incentives,
    base_ap_net_sales_value,
    base_ap_retro,
    base_ap_avp_disc,
    base_ap_variable_trade,
    base_ap_promo_fixed_funding,
    base_ap_range_support_incentives,
    base_ap_net_net_sales_value,
    base_ap_direct_shopper_marketing,
    base_ap_other_direct_payments,
    base_ap_indirect_shopper_marketing,
    base_ap_other_indirect_payments,
    base_ap_fixed_trade_cust_invoiced,
    base_ap_total_trade_cust_invoiced,
    base_ap_fixed_trade_non_cust_invoiced,
    base_ap_total_trade,
    base_ap_net_realisable_revenue,
    base_ap_tot_prime_cost_standard,
    base_ap_gross_margin_standard,
    base_ap_gcat_standard,
    base_manso_tot_vol_kg,
    base_manso_gross_sales_value,
    base_manso_range_support_allowance,
    base_manso_everyday_low_prices,
    base_manso_permanent_disc,
    base_manso_off_invoice_disc,
    base_manso_invoiced_sales_value,
    base_manso_early_settlement_disc,
    base_manso_growth_incentives,
    base_manso_net_sales_value,
    base_manso_retro,
    base_manso_avp_disc,
    base_manso_variable_trade,
    base_manso_promo_fixed_funding,
    base_manso_range_support_incentives,
    base_manso_net_net_sales_value,
    base_manso_direct_shopper_marketing,
    base_manso_other_direct_payments,
    base_manso_indirect_shopper_marketing,
    base_manso_other_indirect_payments,
    base_manso_fixed_trade_cust_invoiced,
    base_manso_total_trade_cust_invoiced,
    base_manso_fixed_trade_non_cust_invoiced,
    base_manso_total_trade,
    base_manso_net_realisable_revenue,
    base_manso_tot_prime_cost_standard,
    base_manso_gross_margin_standard,
    base_manso_gcat_standard,
    base_retail_tot_vol_kg,
    base_ap_retail_revenue_mrrsp,
    base_ap_retail_revenue_rsp,
    base_ap_retail_revenue_net,
    base_ap_retail_cost_of_sales,
    base_ap_retail_retailer_retro_funding,
    base_ap_retail_margin_excl_fixed_funding,
    base_ap_retail_promo_fixed_spend,
    base_ap_retail_total_spend,
    base_ap_retail_margin_incl_fixed_funding,
    base_ap_retail_revenue_net_excl_mrrsp,
    base_ap_retail_revenue_net_excl_rsp,
    forecast_tot_vol_kg,
    forecast_ap_gross_sales_value,
    forecast_ap_range_support_allowance,
    forecast_ap_everyday_low_prices,
    forecast_ap_permanent_disc,
    forecast_ap_off_invoice_disc,
    forecast_ap_invoiced_sales_value,
    forecast_ap_early_settlement_disc,
    forecast_ap_growth_incentives,
    forecast_ap_net_sales_value,
    forecast_ap_retro,
    forecast_ap_avp_disc,
    forecast_ap_variable_trade,
    forecast_ap_promo_fixed_funding,
    forecast_ap_range_support_incentives,
    forecast_ap_net_net_sales_value,
    forecast_ap_direct_shopper_marketing,
    forecast_ap_other_direct_payments,
    forecast_ap_indirect_shopper_marketing,
    forecast_ap_other_indirect_payments,
    forecast_ap_fixed_trade_cust_invoiced,
    forecast_ap_total_trade_cust_invoiced,
    forecast_ap_fixed_trade_non_cust_invoiced,
    forecast_ap_total_trade,
    forecast_ap_total_trade_gbp,
    forecast_ap_net_realisable_revenue,
    forecast_ap_tot_prime_cost_standard,
    forecast_ap_gross_margin_standard,
    forecast_ap_gcat_standard,
    forecast_manso_tot_vol_kg,
    forecast_manso_gross_sales_value,
    forecast_manso_range_support_allowance,
    forecast_manso_everyday_low_prices,
    forecast_manso_permanent_disc,
    forecast_manso_off_invoice_disc,
    forecast_manso_invoiced_sales_value,
    forecast_manso_early_settlement_disc,
    forecast_manso_growth_incentives,
    forecast_manso_net_sales_value,
    forecast_manso_retro,
    forecast_manso_avp_disc,
    forecast_manso_variable_trade,
    forecast_manso_promo_fixed_funding,
    forecast_manso_range_support_incentives,
    forecast_manso_net_net_sales_value,
    forecast_manso_direct_shopper_marketing,
    forecast_manso_other_direct_payments,
    forecast_manso_indirect_shopper_marketing,
    forecast_manso_other_indirect_payments,
    forecast_manso_fixed_trade_cust_invoiced,
    forecast_manso_total_trade_cust_invoiced,
    forecast_manso_fixed_trade_non_cust_invoiced,
    forecast_manso_total_trade,
    forecast_manso_net_realisable_revenue,
    forecast_manso_tot_prime_cost_standard,
    forecast_manso_gross_margin_standard,
    forecast_manso_gcat_standard,
    forecast_retail_tot_vol_kg,
    forecast_ap_retail_revenue_mrrsp,
    forecast_ap_retail_revenue_rsp,
    forecast_ap_retail_revenue_net,
    forecast_ap_retail_cost_of_sales,
    forecast_ap_retail_retailer_retro_funding,
    forecast_ap_retail_margin_excl_fixed_funding,
    forecast_ap_retail_promo_fixed_spend,
    forecast_ap_retail_total_spend,
    forecast_ap_retail_margin_incl_fixed_funding,
    forecast_ap_retail_revenue_net_excl_mrrsp,
    forecast_ap_retail_revenue_net_excl_rsp,
    si_b_vol_kg,
    si_a_vol_kg,
    si_t_vol_kg,
    si_m_vol_kg,
    si_i_vol_kg,
    so_b_vol_kg,
    so_a_vol_kg,
    so_t_vol_kg,
    so_m_vol_kg,
    so_i_vol_kg,
    si_cannib_vol_kg,
    so_cannib_vol_kg,
    si_cannib_basevol_kg,
    so_cannib_basevol_kg,
    si_cannib_loss_vol_kg,
    so_cannib_loss_vol_kg,
    si_predip_vol_kg,
    si_postdip_vol_kg,
    so_predip_vol_kg,
    so_postdip_vol_kg,
    si_predip_basevol_kg,
    si_postdip_basevol_kg,
    so_predip_basevol_kg,
    so_postdip_basevol_kg,
    actuals_tot_vol_ca,
    actuals_tot_vol_ul,
    base_tot_vol_ca,
    base_tot_vol_ul,
    forecast_tot_vol_ca,
    forecast_tot_vol_ul,
    v_ca_kg_conv,
    prm_rpt_customer_code,
    prm_rpt_customer_guid,
    promo_code,
    {{
        dbt_utils.surrogate_key(
            [
                "source_system",
                "promo_idx",
                "plan_source_customer_code",
                "source_item_identifier",
                "calendar_date",
                "snapshot_date",
                "prm_rpt_customer_code",
            ]
        )
    }} as unique_key
from final
