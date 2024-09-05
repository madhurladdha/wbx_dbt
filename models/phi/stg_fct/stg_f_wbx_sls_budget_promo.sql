{{ config(
    enabled=false,
    tags=["sales","adhoc", "budget", "promotion","sls_budget_promo"]) }}

with
    prm_fact as (select * from {{ ref("src_exc_fact_promotion_pandl_promoskuday") }}),
    dp as (select * from {{ ref("src_exc_dim_promotions") }}),
    prd_dim as (select * from {{ ref("src_exc_dim_pc_product") }}),
    prd_cust as (select * from {{ ref("src_exc_dim_pc_customer") }}),
    dim_date as (select * from {{ ref("src_exc_dim_promotion_dates") }}),
    fact_date as (select * from {{ ref("src_exc_fact_promotion_dates") }}),
    buy_in_start as (
        select fact_date.promo_idx, fact_date.promodate_value
        from dim_date
        join fact_date on dim_date.promodate_idx = fact_date.promodate_idx
        where upper(promodate_name) = 'BUY-IN START'
    ),
    buy_in_end as (
        select fact_date.promo_idx, fact_date.promodate_value
        from dim_date
        join fact_date on dim_date.promodate_idx = fact_date.promodate_idx
        where upper(promodate_name) = 'BUY-IN END'
    ),
    -- for curr conv start
    curr_opt as (select * from {{ ref("src_exc_dim_currency_exchange_options") }}),
    curr_rate as (select * from {{ ref("src_exc_fact_currency_exchange_rate") }}),
    dim_curr as (select * from {{ ref("src_exc_dim_currency") }}),
    curr_x as (
        select
            curr_to.currency_code as from_curr,
            curr_from.currency_code as to_curr,
            curr_opt.currency_from_idx as from_curr_idx,
            curr_opt.currency_to_idx as to_curr_idx,
            curr_rate.valid_from_date as eff_start_date,
            curr_rate.valid_to_date as eff_end_date,
            curr_rate.value as conversion_rate,
            1 / curr_rate.value as inversion_rate
        from curr_opt
        inner join curr_rate on curr_opt.option_idx = curr_rate.option_idx
        inner join dim_curr curr_to on curr_opt.currency_from_idx = curr_to.currency_idx
        inner join
            dim_curr curr_from on curr_opt.currency_to_idx = curr_from.currency_idx
    ),
    -- curr conv end
    dim_wbx_sls_promo_cust_sublevel as (
        select * from {{ ref("dim_wbx_sls_promo_cust_sublevel") }}
    ),
    scen_fact as (select * from {{ ref("src_exc_fact_promotion_scenario") }}),
    scen as (select * from {{ ref("src_exc_dim_scenario") }}),
    scen_xref as (select * from {{ ref("src_sls_wtx_budget_scen_xref") }}),  -- using direct source as it is updated/inserted manually
    promo_calc_percent as (
        select
            prm_fact.promo_idx,
            prm_fact.cust_idx,
            prd_cust.code as plan_source_customer_code,
            (
                case
                    when prm_cust_sub_exclude.promo_code is null
                    then nvl(prm_cust_sub.cust_code, prd_cust.code)
                    else prd_cust.code
                end
            ) as prm_rpt_customer_code,
            prm_fact.sku_idx,
            prm_fact.day_idx,
            (
                case
                    when prm_cust_sub_exclude.promo_code is null
                    then 1 / nvl(prm_cust_sub.cust_count, 1)
                    else 1
                end
            ) as perc_applied_vols,
            (
                case
                    when iscannibsku = true and si_t_vol_cse <> 0
                    then (si_i_vol_cse / si_t_vol_cse)
                    else 1
                end
            )
            * nvl(curr_x.conversion_rate, 1)
            * (
                case
                    when prm_cust_sub_exclude.promo_code is null
                    then 1 / nvl(prm_cust_sub.cust_count, 1)
                    else 1
                end
            ) as perc_applied_amts
        from prm_fact
        inner join dp on prm_fact.promo_idx = dp.promo_idx
        left outer join prd_dim on prd_dim.idx = prm_fact.sku_idx
        left outer join prd_cust on prd_cust.idx = prm_fact.cust_idx
        left join buy_in_start on buy_in_start.promo_idx = prm_fact.promo_idx
        left join buy_in_end on buy_in_end.promo_idx = prm_fact.promo_idx
        left outer join
            curr_x
            on prd_cust.currency_idx = curr_x.from_curr_idx
            and 'GBP' = curr_x.to_curr
            and date(prm_fact.day_idx, 'YYYYMMDD') >= curr_x.eff_start_date
            and date(prm_fact.day_idx, 'YYYYMMDD') <= curr_x.eff_end_date
        left outer join
            dim_wbx_sls_promo_cust_sublevel prm_cust_sub
            on prm_cust_sub.promo_idx = prm_fact.promo_idx
            and prm_cust_sub.custlevel_name = 'Branch'
        /* 	If there is a Trade Type Row in this table it implies it is NOT at SubLevel customer, but meant to be purely Trade Type (Planning customer).  So exclude these.
	This is to handle a nuance in the Exceedra (E3) data and how they converted Sublevel customers from E2.
*/
        left outer join
            dim_wbx_sls_promo_cust_sublevel prm_cust_sub_exclude
            on prm_cust_sub_exclude.promo_idx = prm_fact.promo_idx
            and prm_cust_sub_exclude.custlevel_name = 'Trade Type'

        -- HJ 30 Aug 2021,added the below Joins to get the PROMOs related to the
        -- BUDGET(FROZEN FORECAST) scenarios
        left join scen_fact on prm_fact.promo_idx = scen_fact.promo_idx
        left join scen on scen_fact.scen_idx = scen.scen_idx
        -- LEFT JOIN WEETABIX."EXC_Dim_Scenario_Status" SCEN_STAT
        -- ON SCEN.SCEN_STATUS_IDx=SCEN_STAT.SCEN_STATUS_IDx
        -- LEFT JOIN WEETABIX."EXC_Dim_Scenario_Types" SCEN_TYPE
        -- on SCEN.SCEN_TYPE_IDx=SCEN_TYPE.SCEN_TYPE_IDx
        -- this is a CROSS REF table maintained for linking Scenarios to the desired
        -- FROZEN_FORECAST values
        left join
            scen_xref
            -- LEFT JOIN EI_RDM.SLS_WTX_BUDGET_SCEN_XREF SCEN_XREF
            on scen.scen_idx = scen_xref.scen_id
        -- WHERE SCEN_STAT.SCEN_STATUS_CODE='CLOSED'
        -- AND SCEN_TYPE.SCEN_TYPE_CODE='SNAPSHOT'
        -- only get the senarios that are latest,have the flag set as 1 in the CROSS
        -- REF
        where
            scen_xref.current_version_flag = 1

            and to_date(to_char(prm_fact.day_idx), 'YYYYMMDD')
            between buy_in_start.promodate_value and buy_in_end.promodate_value
    ),
    stage as (
        select
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            dp.promo_code,
            prm_fact.promo_idx,
            prm_fact.cust_idx,
            prd_cust.code as plan_source_customer_code,
            pcp.prm_rpt_customer_code as prm_rpt_customer_code,
            prm_fact.sku_idx,
            prd_dim.code as source_item_identifier,
            prm_fact.day_idx,
            prm_fact.reportingsku_idx,
            prm_fact.ispromosku,
            prm_fact.iscannibsku,
            prm_fact.issi_prepromoday,
            prm_fact.issi_onpromoday,
            prm_fact.issi_postpromoday,
            prm_fact.isso_prepromoday,
            prm_fact.isso_onpromoday,
            prm_fact.isso_postpromoday,
            scen.scen_idx as scen_idx,
            scen.scen_name as scen_name,
            scen.scen_code as scen_code,
            scen_xref.frozen_forecast as frozen_forecast,
            nvl(prm_fact.si_b_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_b_vol_cse,
            nvl(prm_fact.si_b_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_b_vol_sgl,
            nvl(prm_fact.si_a_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_a_vol_cse,
            nvl(prm_fact.si_a_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_a_vol_sgl,
            nvl(prm_fact.si_t_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_t_vol_cse,
            nvl(prm_fact.si_t_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_t_vol_sgl,
            nvl(prm_fact.si_m_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_m_vol_cse,
            nvl(prm_fact.si_m_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_m_vol_sgl,
            nvl(prm_fact.si_i_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_i_vol_cse,
            nvl(prm_fact.si_i_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_i_vol_sgl,
            nvl(prm_fact.so_b_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_b_vol_cse,
            nvl(prm_fact.so_b_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_b_vol_sgl,
            nvl(prm_fact.so_a_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_a_vol_cse,
            nvl(prm_fact.so_a_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_a_vol_sgl,
            nvl(prm_fact.so_t_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_t_vol_cse,
            nvl(prm_fact.so_t_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_t_vol_sgl,
            nvl(prm_fact.so_m_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_m_vol_cse,
            nvl(prm_fact.so_m_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_m_vol_sgl,
            nvl(prm_fact.so_i_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_i_vol_cse,
            nvl(prm_fact.so_i_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_i_vol_sgl,
            nvl(prm_fact.si_cannib_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_cannib_vol_cse,
            nvl(prm_fact.si_cannib_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_cannib_vol_sgl,
            nvl(prm_fact.so_cannib_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_cannib_vol_cse,
            nvl(prm_fact.so_cannib_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_cannib_vol_sgl,
            nvl(prm_fact.si_cannib_basevol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_cannib_basevol_cse,
            nvl(prm_fact.si_cannib_basevol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_cannib_basevol_sgl,
            nvl(prm_fact.so_cannib_basevol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_cannib_basevol_cse,
            nvl(prm_fact.so_cannib_basevol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_cannib_basevol_sgl,
            nvl(prm_fact.si_cannib_loss_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_cannib_loss_vol_cse,
            nvl(prm_fact.si_cannib_loss_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_cannib_loss_vol_sgl,
            nvl(prm_fact.so_cannib_loss_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_cannib_loss_vol_cse,
            nvl(prm_fact.so_cannib_loss_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_cannib_loss_vol_sgl,
            nvl(prm_fact.si_predip_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_predip_vol_cse,
            nvl(prm_fact.si_predip_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_predip_vol_sgl,
            nvl(prm_fact.si_postdip_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_postdip_vol_cse,
            nvl(prm_fact.si_postdip_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_postdip_vol_sgl,
            nvl(prm_fact.so_predip_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_predip_vol_cse,
            nvl(prm_fact.so_predip_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_predip_vol_sgl,
            nvl(prm_fact.so_postdip_vol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_postdip_vol_cse,
            nvl(prm_fact.so_postdip_vol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_postdip_vol_sgl,
            nvl(prm_fact.si_predip_basevol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_predip_basevol_cse,
            nvl(prm_fact.si_predip_basevol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_predip_basevol_sgl,
            nvl(prm_fact.si_postdip_basevol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_postdip_basevol_cse,
            nvl(prm_fact.si_postdip_basevol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as si_postdip_basevol_sgl,
            nvl(prm_fact.so_predip_basevol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_predip_basevol_cse,
            nvl(prm_fact.so_predip_basevol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_predip_basevol_sgl,
            nvl(prm_fact.so_postdip_basevol_cse, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_postdip_basevol_cse,
            nvl(prm_fact.so_postdip_basevol_sgl, 0)
            * nvl(pcp.perc_applied_vols, 1) as so_postdip_basevol_sgl,
            nvl(prm_fact.postpromodippercent_si, 0)
            * nvl(pcp.perc_applied_vols, 1) as postpromodippercent_si,
            nvl(prm_fact.postpromodippercent_so, 0)
            * nvl(pcp.perc_applied_vols, 1) as postpromodippercent_so,
            nvl(prm_fact.prepromodippercent_si, 0)
            * nvl(pcp.perc_applied_vols, 1) as prepromodippercent_si,
            nvl(prm_fact.prepromodippercent_so, 0)
            * nvl(pcp.perc_applied_vols, 1) as prepromodippercent_so,
            nvl(prm_fact.onpromophasingpercent_si, 0)
            * nvl(pcp.perc_applied_vols, 1) as onpromophasingpercent_si,
            nvl(prm_fact.onpromophasingpercent_so, 0)
            * nvl(pcp.perc_applied_vols, 1) as onpromophasingpercent_so,
            nvl(prm_fact.robfundingrequired, 0) as robfundingrequired,
            nvl(prm_fact.a_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as a_tot_vol_kg,
            nvl(prm_fact.a_ap_gross_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_gross_sales_value,
            nvl(prm_fact.a_ap_range_support_allowance, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_range_support_allowance,
            nvl(prm_fact.a_ap_everyday_low_prices, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_everyday_low_prices,
            nvl(prm_fact.a_ap_permanent_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_permanent_disc,
            nvl(prm_fact.a_ap_off_invoice_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_off_invoice_disc,
            nvl(prm_fact.a_ap_invoiced_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_invoiced_sales_value,
            nvl(prm_fact.a_ap_early_settlement_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_early_settlement_disc,
            nvl(prm_fact.a_ap_growth_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_growth_incentives,
            nvl(prm_fact.a_ap_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_net_sales_value,
            nvl(prm_fact.a_ap_retro, 0) * nvl(pcp.perc_applied_amts, 1) as a_ap_retro,
            nvl(prm_fact.a_ap_avp_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_avp_disc,
            nvl(prm_fact.a_ap_variable_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_variable_trade,
            nvl(prm_fact.a_ap_promo_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_promo_fixed_funding,
            nvl(prm_fact.a_ap_range_support_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_range_support_incentives,
            nvl(prm_fact.a_ap_net_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_net_net_sales_value,
            nvl(prm_fact.a_ap_direct_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_direct_shopper_marketing,
            nvl(prm_fact.a_ap_other_direct_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_other_direct_payments,
            nvl(prm_fact.a_ap_indirect_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_indirect_shopper_marketing,
            nvl(prm_fact.a_ap_other_indirect_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_other_indirect_payments,
            nvl(prm_fact.a_ap_fixed_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_fixed_trade_cust_invoiced,
            nvl(prm_fact.a_ap_total_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_total_trade_cust_invoiced,
            nvl(prm_fact.a_ap_fixed_trade_non_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_fixed_trade_non_cust_invoiced,
            nvl(prm_fact.a_ap_total_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_total_trade,
            nvl(prm_fact.a_ap_net_realisable_revenue, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_net_realisable_revenue,
            nvl(prm_fact.a_ap_tot_prime_cost_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_tot_prime_cost_standard,
            nvl(prm_fact.a_ap_gross_margin_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_gross_margin_standard,
            nvl(prm_fact.a_ap_gcat_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_gcat_standard,
            nvl(prm_fact.a_manso_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as a_manso_tot_vol_kg,
            nvl(prm_fact.a_manso_gross_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_gross_sales_value,
            nvl(prm_fact.a_manso_range_support_allowance, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_range_support_allowance,
            nvl(prm_fact.a_manso_everyday_low_prices, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_everyday_low_prices,
            nvl(prm_fact.a_manso_permanent_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_permanent_disc,
            nvl(prm_fact.a_manso_off_invoice_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_off_invoice_disc,
            nvl(prm_fact.a_manso_invoiced_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_invoiced_sales_value,
            nvl(prm_fact.a_manso_early_settlement_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_early_settlement_disc,
            nvl(prm_fact.a_manso_growth_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_growth_incentives,
            nvl(prm_fact.a_manso_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_net_sales_value,
            nvl(prm_fact.a_manso_retro, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_retro,
            nvl(prm_fact.a_manso_avp_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_avp_disc,
            nvl(prm_fact.a_manso_variable_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_variable_trade,
            nvl(prm_fact.a_manso_promo_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_promo_fixed_funding,
            nvl(prm_fact.a_manso_range_support_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_range_support_incentives,
            nvl(prm_fact.a_manso_net_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_net_net_sales_value,
            nvl(prm_fact.a_manso_direct_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_direct_shopper_marketing,
            nvl(prm_fact.a_manso_other_direct_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_other_direct_payments,
            nvl(prm_fact.a_manso_indirect_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_indirect_shopper_marketing,
            nvl(prm_fact.a_manso_other_indirect_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_other_indirect_payments,
            nvl(prm_fact.a_manso_fixed_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_fixed_trade_cust_invoiced,
            nvl(prm_fact.a_manso_total_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_total_trade_cust_invoiced,
            nvl(prm_fact.a_manso_fixed_trade_non_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_fixed_trade_non_cust_invoiced,
            nvl(prm_fact.a_manso_total_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_total_trade,
            nvl(prm_fact.a_manso_net_realisable_revenue, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_net_realisable_revenue,
            nvl(prm_fact.a_manso_tot_prime_cost_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_tot_prime_cost_standard,
            nvl(prm_fact.a_manso_gross_margin_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_gross_margin_standard,
            nvl(prm_fact.a_manso_gcat_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_manso_gcat_standard,
            nvl(prm_fact.a_retail_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as a_retail_tot_vol_kg,
            nvl(prm_fact.a_ap_retail_revenue_mrrsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_revenue_mrrsp,
            nvl(prm_fact.a_ap_retail_revenue_rsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_revenue_rsp,
            nvl(prm_fact.a_ap_retail_revenue_net, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_revenue_net,
            nvl(prm_fact.a_ap_retail_cost_of_sales, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_cost_of_sales,
            nvl(prm_fact.a_ap_retail_retailer_retro_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_retailer_retro_funding,
            nvl(prm_fact.a_ap_retail_margin_excl_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_margin_excl_fixed_funding,
            nvl(prm_fact.a_ap_retail_promo_fixed_spend, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_promo_fixed_spend,
            nvl(prm_fact.a_ap_retail_total_spend, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_total_spend,
            nvl(prm_fact.a_ap_retail_margin_incl_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_margin_incl_fixed_funding,
            nvl(prm_fact.a_ap_retail_revenue_net_excl_mrrsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_revenue_net_excl_mrrsp,
            nvl(prm_fact.a_ap_retail_revenue_net_excl_rsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as a_ap_retail_revenue_net_excl_rsp,
            nvl(prm_fact.b_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as b_tot_vol_kg,
            nvl(prm_fact.b_ap_gross_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_gross_sales_value,
            nvl(prm_fact.b_ap_range_support_allowance, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_range_support_allowance,
            nvl(prm_fact.b_ap_everyday_low_prices, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_everyday_low_prices,
            nvl(prm_fact.b_ap_permanent_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_permanent_disc,
            nvl(prm_fact.b_ap_off_invoice_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_off_invoice_disc,
            nvl(prm_fact.b_ap_invoiced_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_invoiced_sales_value,
            nvl(prm_fact.b_ap_early_settlement_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_early_settlement_disc,
            nvl(prm_fact.b_ap_growth_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_growth_incentives,
            nvl(prm_fact.b_ap_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_net_sales_value,
            nvl(prm_fact.b_ap_retro, 0) * nvl(pcp.perc_applied_amts, 1) as b_ap_retro,
            nvl(prm_fact.b_ap_avp_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_avp_disc,
            nvl(prm_fact.b_ap_variable_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_variable_trade,
            nvl(prm_fact.b_ap_promo_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_promo_fixed_funding,
            nvl(prm_fact.b_ap_range_support_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_range_support_incentives,
            nvl(prm_fact.b_ap_net_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_net_net_sales_value,
            nvl(prm_fact.b_ap_direct_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_direct_shopper_marketing,
            nvl(prm_fact.b_ap_other_direct_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_other_direct_payments,
            nvl(prm_fact.b_ap_indirect_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_indirect_shopper_marketing,
            nvl(prm_fact.b_ap_other_indirect_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_other_indirect_payments,
            nvl(prm_fact.b_ap_fixed_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_fixed_trade_cust_invoiced,
            nvl(prm_fact.b_ap_total_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_total_trade_cust_invoiced,
            nvl(prm_fact.b_ap_fixed_trade_non_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_fixed_trade_non_cust_invoiced,
            nvl(prm_fact.b_ap_total_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_total_trade,
            nvl(prm_fact.b_ap_net_realisable_revenue, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_net_realisable_revenue,
            nvl(prm_fact.b_ap_tot_prime_cost_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_tot_prime_cost_standard,
            nvl(prm_fact.b_ap_gross_margin_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_gross_margin_standard,
            nvl(prm_fact.b_ap_gcat_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_gcat_standard,
            nvl(prm_fact.b_manso_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as b_manso_tot_vol_kg,
            nvl(prm_fact.b_manso_gross_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_gross_sales_value,
            nvl(prm_fact.b_manso_range_support_allowance, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_range_support_allowance,
            nvl(prm_fact.b_manso_everyday_low_prices, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_everyday_low_prices,
            nvl(prm_fact.b_manso_permanent_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_permanent_disc,
            nvl(prm_fact.b_manso_off_invoice_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_off_invoice_disc,
            nvl(prm_fact.b_manso_invoiced_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_invoiced_sales_value,
            nvl(prm_fact.b_manso_early_settlement_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_early_settlement_disc,
            nvl(prm_fact.b_manso_growth_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_growth_incentives,
            nvl(prm_fact.b_manso_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_net_sales_value,
            nvl(prm_fact.b_manso_retro, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_retro,
            nvl(prm_fact.b_manso_avp_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_avp_disc,
            nvl(prm_fact.b_manso_variable_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_variable_trade,
            nvl(prm_fact.b_manso_promo_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_promo_fixed_funding,
            nvl(prm_fact.b_manso_range_support_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_range_support_incentives,
            nvl(prm_fact.b_manso_net_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_net_net_sales_value,
            nvl(prm_fact.b_manso_direct_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_direct_shopper_marketing,
            nvl(prm_fact.b_manso_other_direct_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_other_direct_payments,
            nvl(prm_fact.b_manso_indirect_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_indirect_shopper_marketing,
            nvl(prm_fact.b_manso_other_indirect_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_other_indirect_payments,
            nvl(prm_fact.b_manso_fixed_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_fixed_trade_cust_invoiced,
            nvl(prm_fact.b_manso_total_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_total_trade_cust_invoiced,
            nvl(prm_fact.b_manso_fixed_trade_non_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_fixed_trade_non_cust_invoiced,
            nvl(prm_fact.b_manso_total_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_total_trade,
            nvl(prm_fact.b_manso_net_realisable_revenue, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_net_realisable_revenue,
            nvl(prm_fact.b_manso_tot_prime_cost_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_tot_prime_cost_standard,
            nvl(prm_fact.b_manso_gross_margin_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_gross_margin_standard,
            nvl(prm_fact.b_manso_gcat_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_manso_gcat_standard,
            nvl(prm_fact.b_retail_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as b_retail_tot_vol_kg,
            nvl(prm_fact.b_ap_retail_revenue_mrrsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_revenue_mrrsp,
            nvl(prm_fact.b_ap_retail_revenue_rsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_revenue_rsp,
            nvl(prm_fact.b_ap_retail_revenue_net, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_revenue_net,
            nvl(prm_fact.b_ap_retail_cost_of_sales, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_cost_of_sales,
            nvl(prm_fact.b_ap_retail_retailer_retro_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_retailer_retro_funding,
            nvl(prm_fact.b_ap_retail_margin_excl_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_margin_excl_fixed_funding,
            nvl(prm_fact.b_ap_retail_promo_fixed_spend, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_promo_fixed_spend,
            nvl(prm_fact.b_ap_retail_total_spend, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_total_spend,
            nvl(prm_fact.b_ap_retail_margin_incl_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_margin_incl_fixed_funding,
            nvl(prm_fact.b_ap_retail_revenue_net_excl_mrrsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_revenue_net_excl_mrrsp,
            nvl(prm_fact.b_ap_retail_revenue_net_excl_rsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as b_ap_retail_revenue_net_excl_rsp,
            nvl(prm_fact.t_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as t_tot_vol_kg,
            nvl(prm_fact.t_ap_gross_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_gross_sales_value,
            nvl(prm_fact.t_ap_range_support_allowance, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_range_support_allowance,
            nvl(prm_fact.t_ap_everyday_low_prices, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_everyday_low_prices,
            nvl(prm_fact.t_ap_permanent_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_permanent_disc,
            nvl(prm_fact.t_ap_off_invoice_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_off_invoice_disc,
            nvl(prm_fact.t_ap_invoiced_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_invoiced_sales_value,
            nvl(prm_fact.t_ap_early_settlement_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_early_settlement_disc,
            nvl(prm_fact.t_ap_growth_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_growth_incentives,
            nvl(prm_fact.t_ap_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_net_sales_value,
            nvl(prm_fact.t_ap_retro, 0) * nvl(pcp.perc_applied_amts, 1) as t_ap_retro,
            nvl(prm_fact.t_ap_avp_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_avp_disc,
            nvl(prm_fact.t_ap_variable_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_variable_trade,
            nvl(prm_fact.t_ap_promo_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_promo_fixed_funding,
            nvl(prm_fact.t_ap_range_support_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_range_support_incentives,
            nvl(prm_fact.t_ap_net_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_net_net_sales_value,
            nvl(prm_fact.t_ap_direct_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_direct_shopper_marketing,
            nvl(prm_fact.t_ap_other_direct_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_other_direct_payments,
            nvl(prm_fact.t_ap_indirect_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_indirect_shopper_marketing,
            nvl(prm_fact.t_ap_other_indirect_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_other_indirect_payments,
            nvl(prm_fact.t_ap_fixed_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_fixed_trade_cust_invoiced,
            nvl(prm_fact.t_ap_total_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_total_trade_cust_invoiced,
            nvl(prm_fact.t_ap_fixed_trade_non_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_fixed_trade_non_cust_invoiced,
            nvl(prm_fact.t_ap_total_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_total_trade,
            nvl(prm_fact.t_ap_total_trade_gbp, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_total_trade_gbp,
            nvl(prm_fact.t_ap_net_realisable_revenue, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_net_realisable_revenue,
            nvl(prm_fact.t_ap_tot_prime_cost_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_tot_prime_cost_standard,
            nvl(prm_fact.t_ap_gross_margin_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_gross_margin_standard,
            nvl(prm_fact.t_ap_gcat_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_gcat_standard,
            nvl(prm_fact.t_manso_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as t_manso_tot_vol_kg,
            nvl(prm_fact.t_manso_gross_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_gross_sales_value,
            nvl(prm_fact.t_manso_range_support_allowance, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_range_support_allowance,
            nvl(prm_fact.t_manso_everyday_low_prices, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_everyday_low_prices,
            nvl(prm_fact.t_manso_permanent_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_permanent_disc,
            nvl(prm_fact.t_manso_off_invoice_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_off_invoice_disc,
            nvl(prm_fact.t_manso_invoiced_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_invoiced_sales_value,
            nvl(prm_fact.t_manso_early_settlement_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_early_settlement_disc,
            nvl(prm_fact.t_manso_growth_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_growth_incentives,
            nvl(prm_fact.t_manso_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_net_sales_value,
            nvl(prm_fact.t_manso_retro, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_retro,
            nvl(prm_fact.t_manso_avp_disc, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_avp_disc,
            nvl(prm_fact.t_manso_variable_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_variable_trade,
            nvl(prm_fact.t_manso_promo_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_promo_fixed_funding,
            nvl(prm_fact.t_manso_range_support_incentives, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_range_support_incentives,
            nvl(prm_fact.t_manso_net_net_sales_value, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_net_net_sales_value,
            nvl(prm_fact.t_manso_direct_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_direct_shopper_marketing,
            nvl(prm_fact.t_manso_other_direct_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_other_direct_payments,
            nvl(prm_fact.t_manso_indirect_shopper_marketing, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_indirect_shopper_marketing,
            nvl(prm_fact.t_manso_other_indirect_payments, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_other_indirect_payments,
            nvl(prm_fact.t_manso_fixed_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_fixed_trade_cust_invoiced,
            nvl(prm_fact.t_manso_total_trade_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_total_trade_cust_invoiced,
            nvl(prm_fact.t_manso_fixed_trade_non_cust_invoiced, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_fixed_trade_non_cust_invoiced,
            nvl(prm_fact.t_manso_total_trade, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_total_trade,
            nvl(prm_fact.t_manso_net_realisable_revenue, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_net_realisable_revenue,
            nvl(prm_fact.t_manso_tot_prime_cost_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_tot_prime_cost_standard,
            nvl(prm_fact.t_manso_gross_margin_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_gross_margin_standard,
            nvl(prm_fact.t_manso_gcat_standard, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_manso_gcat_standard,
            nvl(prm_fact.t_retail_tot_vol_kg, 0)
            * nvl(pcp.perc_applied_vols, 1) as t_retail_tot_vol_kg,
            nvl(prm_fact.t_ap_retail_revenue_mrrsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_revenue_mrrsp,
            nvl(prm_fact.t_ap_retail_revenue_rsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_revenue_rsp,
            nvl(prm_fact.t_ap_retail_revenue_net, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_revenue_net,
            nvl(prm_fact.t_ap_retail_cost_of_sales, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_cost_of_sales,
            nvl(prm_fact.t_ap_retail_retailer_retro_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_retailer_retro_funding,
            nvl(prm_fact.t_ap_retail_margin_excl_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_margin_excl_fixed_funding,
            nvl(prm_fact.t_ap_retail_promo_fixed_spend, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_promo_fixed_spend,
            nvl(prm_fact.t_ap_retail_total_spend, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_total_spend,
            nvl(prm_fact.t_ap_retail_margin_incl_fixed_funding, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_margin_incl_fixed_funding,
            nvl(prm_fact.t_ap_retail_revenue_net_excl_mrrsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_revenue_net_excl_mrrsp,
            nvl(prm_fact.t_ap_retail_revenue_net_excl_rsp, 0)
            * nvl(pcp.perc_applied_amts, 1) as t_ap_retail_revenue_net_excl_rsp
        from prm_fact
        inner join dp on prm_fact.promo_idx = dp.promo_idx
        left outer join prd_dim on prd_dim.idx = prm_fact.sku_idx
        left outer join prd_cust on prd_cust.idx = prm_fact.cust_idx
        left join buy_in_start on buy_in_start.promo_idx = prm_fact.promo_idx
        left join buy_in_end on buy_in_end.promo_idx = prm_fact.promo_idx
        left outer join
            curr_x
            on prd_cust.currency_idx = curr_x.from_curr_idx
            and 'GBP' = curr_x.to_curr
            and date(prm_fact.day_idx, 'YYYYMMDD') >= curr_x.eff_start_date
            and date(prm_fact.day_idx, 'YYYYMMDD') <= curr_x.eff_end_date
        /* Removed the join(s) the the Sublevel customer views as they currently are only needed in the first step.  By then joining to PROMO_CALC_PERCENT by all keys except the SubLevel customer here, we should get the desired cartesian for such Promos
	and the correct 1 to 1 join when it is not Sublevel.
*/
        inner join
            promo_calc_percent pcp
            on prm_fact.promo_idx = pcp.promo_idx
            and prm_fact.cust_idx = pcp.cust_idx
            and prm_fact.sku_idx = pcp.sku_idx
            and prm_fact.day_idx = pcp.day_idx
        -- AND (CASE WHEN PRM_CUST_SUB_EXCLUDE.PROMO_CODE IS NULL THEN
        -- NVL(PRM_CUST_SUB.CUST_CODE,PRD_CUST.CODE) ELSE PRD_CUST.CODE
        -- END)=PCP.PRM_RPT_CUSTOMER_CODE
        -- HJ 30 Aug 2021,added the below Joins to get the PROMOs related to the
        -- BUDGET(FROZEN FORECAST) scenarios
        left join scen_fact on prm_fact.promo_idx = scen_fact.promo_idx
        left join scen on scen_fact.scen_idx = scen.scen_idx
        -- LEFT JOIN WEETABIX."EXC_Dim_Scenario_Status" SCEN_STAT
        -- ON SCEN.SCEN_STATUS_IDx=SCEN_STAT.SCEN_STATUS_IDx
        -- LEFT JOIN WEETABIX."EXC_Dim_Scenario_Types" SCEN_TYPE
        -- on SCEN.SCEN_TYPE_IDx=SCEN_TYPE.SCEN_TYPE_IDx
        -- this is a CROSS REF table maintained for linking Scenarios to the desired
        -- FROZEN_FORECAST values
        left join scen_xref on scen.scen_idx = scen_xref.scen_id
        -- WHERE SCEN_STAT.SCEN_STATUS_CODE='CLOSED'
        -- AND SCEN_TYPE.SCEN_TYPE_CODE='SNAPSHOT'
        -- only get the senarios that are latest,have the flag set as 1 in the CROSS
        -- REF
        where
            scen_xref.current_version_flag = 1
            and to_date(to_char(prm_fact.day_idx), 'YYYYMMDD')
            between buy_in_start.promodate_value and buy_in_end.promodate_value
    ),
    final as (
        select
            source_system,
            promo_idx,
            cust_idx,
            plan_source_customer_code,
            sku_idx,
            source_item_identifier,
            day_idx,
            reportingsku_idx,
            ispromosku,
            iscannibsku,
            issi_prepromoday,
            issi_onpromoday,
            issi_postpromoday,
            isso_prepromoday,
            isso_onpromoday,
            isso_postpromoday,
            scen_idx,
            scen_name,
            scen_code,
            frozen_forecast,
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
            a_tot_vol_kg,
            a_ap_gross_sales_value,
            a_ap_range_support_allowance,
            a_ap_everyday_low_prices,
            a_ap_permanent_disc,
            a_ap_off_invoice_disc,
            a_ap_invoiced_sales_value,
            a_ap_early_settlement_disc,
            a_ap_growth_incentives,
            a_ap_net_sales_value,
            a_ap_retro,
            a_ap_avp_disc,
            a_ap_variable_trade,
            a_ap_promo_fixed_funding,
            a_ap_range_support_incentives,
            a_ap_net_net_sales_value,
            a_ap_direct_shopper_marketing,
            a_ap_other_direct_payments,
            a_ap_indirect_shopper_marketing,
            a_ap_other_indirect_payments,
            a_ap_fixed_trade_cust_invoiced,
            a_ap_total_trade_cust_invoiced,
            a_ap_fixed_trade_non_cust_invoiced,
            a_ap_total_trade,
            a_ap_net_realisable_revenue,
            a_ap_tot_prime_cost_standard,
            a_ap_gross_margin_standard,
            a_ap_gcat_standard,
            a_manso_tot_vol_kg,
            a_manso_gross_sales_value,
            a_manso_range_support_allowance,
            a_manso_everyday_low_prices,
            a_manso_permanent_disc,
            a_manso_off_invoice_disc,
            a_manso_invoiced_sales_value,
            a_manso_early_settlement_disc,
            a_manso_growth_incentives,
            a_manso_net_sales_value,
            a_manso_retro,
            a_manso_avp_disc,
            a_manso_variable_trade,
            a_manso_promo_fixed_funding,
            a_manso_range_support_incentives,
            a_manso_net_net_sales_value,
            a_manso_direct_shopper_marketing,
            a_manso_other_direct_payments,
            a_manso_indirect_shopper_marketing,
            a_manso_other_indirect_payments,
            a_manso_fixed_trade_cust_invoiced,
            a_manso_total_trade_cust_invoiced,
            a_manso_fixed_trade_non_cust_invoiced,
            a_manso_total_trade,
            a_manso_net_realisable_revenue,
            a_manso_tot_prime_cost_standard,
            a_manso_gross_margin_standard,
            a_manso_gcat_standard,
            a_retail_tot_vol_kg,
            a_ap_retail_revenue_mrrsp,
            a_ap_retail_revenue_rsp,
            a_ap_retail_revenue_net,
            a_ap_retail_cost_of_sales,
            a_ap_retail_retailer_retro_funding,
            a_ap_retail_margin_excl_fixed_funding,
            a_ap_retail_promo_fixed_spend,
            a_ap_retail_total_spend,
            a_ap_retail_margin_incl_fixed_funding,
            a_ap_retail_revenue_net_excl_mrrsp,
            a_ap_retail_revenue_net_excl_rsp,
            b_tot_vol_kg,
            b_ap_gross_sales_value,
            b_ap_range_support_allowance,
            b_ap_everyday_low_prices,
            b_ap_permanent_disc,
            b_ap_off_invoice_disc,
            b_ap_invoiced_sales_value,
            b_ap_early_settlement_disc,
            b_ap_growth_incentives,
            b_ap_net_sales_value,
            b_ap_retro,
            b_ap_avp_disc,
            b_ap_variable_trade,
            b_ap_promo_fixed_funding,
            b_ap_range_support_incentives,
            b_ap_net_net_sales_value,
            b_ap_direct_shopper_marketing,
            b_ap_other_direct_payments,
            b_ap_indirect_shopper_marketing,
            b_ap_other_indirect_payments,
            b_ap_fixed_trade_cust_invoiced,
            b_ap_total_trade_cust_invoiced,
            b_ap_fixed_trade_non_cust_invoiced,
            b_ap_total_trade,
            b_ap_net_realisable_revenue,
            b_ap_tot_prime_cost_standard,
            b_ap_gross_margin_standard,
            b_ap_gcat_standard,
            b_manso_tot_vol_kg,
            b_manso_gross_sales_value,
            b_manso_range_support_allowance,
            b_manso_everyday_low_prices,
            b_manso_permanent_disc,
            b_manso_off_invoice_disc,
            b_manso_invoiced_sales_value,
            b_manso_early_settlement_disc,
            b_manso_growth_incentives,
            b_manso_net_sales_value,
            b_manso_retro,
            b_manso_avp_disc,
            b_manso_variable_trade,
            b_manso_promo_fixed_funding,
            b_manso_range_support_incentives,
            b_manso_net_net_sales_value,
            b_manso_direct_shopper_marketing,
            b_manso_other_direct_payments,
            b_manso_indirect_shopper_marketing,
            b_manso_other_indirect_payments,
            b_manso_fixed_trade_cust_invoiced,
            b_manso_total_trade_cust_invoiced,
            b_manso_fixed_trade_non_cust_invoiced,
            b_manso_total_trade,
            b_manso_net_realisable_revenue,
            b_manso_tot_prime_cost_standard,
            b_manso_gross_margin_standard,
            b_manso_gcat_standard,
            b_retail_tot_vol_kg,
            b_ap_retail_revenue_mrrsp,
            b_ap_retail_revenue_rsp,
            b_ap_retail_revenue_net,
            b_ap_retail_cost_of_sales,
            b_ap_retail_retailer_retro_funding,
            b_ap_retail_margin_excl_fixed_funding,
            b_ap_retail_promo_fixed_spend,
            b_ap_retail_total_spend,
            b_ap_retail_margin_incl_fixed_funding,
            b_ap_retail_revenue_net_excl_mrrsp,
            b_ap_retail_revenue_net_excl_rsp,
            t_tot_vol_kg,
            t_ap_gross_sales_value,
            t_ap_range_support_allowance,
            t_ap_everyday_low_prices,
            t_ap_permanent_disc,
            t_ap_off_invoice_disc,
            t_ap_invoiced_sales_value,
            t_ap_early_settlement_disc,
            t_ap_growth_incentives,
            t_ap_net_sales_value,
            t_ap_retro,
            t_ap_avp_disc,
            t_ap_variable_trade,
            t_ap_promo_fixed_funding,
            t_ap_range_support_incentives,
            t_ap_net_net_sales_value,
            t_ap_direct_shopper_marketing,
            t_ap_other_direct_payments,
            t_ap_indirect_shopper_marketing,
            t_ap_other_indirect_payments,
            t_ap_fixed_trade_cust_invoiced,
            t_ap_total_trade_cust_invoiced,
            t_ap_fixed_trade_non_cust_invoiced,
            t_ap_total_trade,
            t_ap_total_trade_gbp,
            t_ap_net_realisable_revenue,
            t_ap_tot_prime_cost_standard,
            t_ap_gross_margin_standard,
            t_ap_gcat_standard,
            t_manso_tot_vol_kg,
            t_manso_gross_sales_value,
            t_manso_range_support_allowance,
            t_manso_everyday_low_prices,
            t_manso_permanent_disc,
            t_manso_off_invoice_disc,
            t_manso_invoiced_sales_value,
            t_manso_early_settlement_disc,
            t_manso_growth_incentives,
            t_manso_net_sales_value,
            t_manso_retro,
            t_manso_avp_disc,
            t_manso_variable_trade,
            t_manso_promo_fixed_funding,
            t_manso_range_support_incentives,
            t_manso_net_net_sales_value,
            t_manso_direct_shopper_marketing,
            t_manso_other_direct_payments,
            t_manso_indirect_shopper_marketing,
            t_manso_other_indirect_payments,
            t_manso_fixed_trade_cust_invoiced,
            t_manso_total_trade_cust_invoiced,
            t_manso_fixed_trade_non_cust_invoiced,
            t_manso_total_trade,
            t_manso_net_realisable_revenue,
            t_manso_tot_prime_cost_standard,
            t_manso_gross_margin_standard,
            t_manso_gcat_standard,
            t_retail_tot_vol_kg,
            t_ap_retail_revenue_mrrsp,
            t_ap_retail_revenue_rsp,
            t_ap_retail_revenue_net,
            t_ap_retail_cost_of_sales,
            t_ap_retail_retailer_retro_funding,
            t_ap_retail_margin_excl_fixed_funding,
            t_ap_retail_promo_fixed_spend,
            t_ap_retail_total_spend,
            t_ap_retail_margin_incl_fixed_funding,
            t_ap_retail_revenue_net_excl_mrrsp,
            t_ap_retail_revenue_net_excl_rsp,
            prm_rpt_customer_code,
            promo_code
        from stage
    )
--adding below part to load just the structure for the first run as there is no data in stage table in prod currently
--second run if any will load entire data
{% if check_table_exists(this.schema, this.table) == "False" %} select * from final where 1=2
{% else %} select * from final
{% endif %}
