{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        enabled=true,
        tags=["adhoc","ax_hist_fact","ax_hist_sales","ax_hist_on_demand"],
    )
}}


with
    source as (

        select * from {{ source("FACTS_FOR_COMPARE", "sls_wtx_budget_promo_fact") }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'

    ),

    renamed as (

        select
            source_system,
            promo_idx,
            {{ dbt_utils.surrogate_key(["source_system", "promo_idx"]) }} as promo_guid,
            cust_idx,
            plan_source_customer_code,
            customer_address_number_guid,
            sku_idx,
            source_item_identifier,
            {{ dbt_utils.surrogate_key(["source_system", "source_item_identifier"]) }}
            as item_guid,
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
            scen_idx,
            scen_name,
            scen_code,
            {{ dbt_utils.surrogate_key(["source_system", "scen_idx"]) }}
            as scenario_guid,
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
            {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "prm_rpt_customer_code",
                        "'CUSTOMER_MAIN'",
                    ]
                )
            }} as prm_rpt_customer_guid,
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
                        "frozen_forecast",
                        "prm_rpt_customer_code",
                    ]
                )
            }} as unique_key

        from source

    )

select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(promo_idx as number(38, 0)) as promo_idx,

    cast(promo_guid as text(255)) as promo_guid,

    cast(cust_idx as number(38, 0)) as cust_idx,

    cast(
        substring(plan_source_customer_code, 1, 255) as text(255)
    ) as plan_source_customer_code,

    cast(customer_address_number_guid as text(255)) as customer_address_number_guid,

    cast(sku_idx as number(38, 0)) as sku_idx,

    cast(
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,

    cast(item_guid as text(255)) as item_guid,

    cast(calendar_date as date) as calendar_date,

    cast(snapshot_date as date) as snapshot_date,

    cast(reportingsku_idx as number(38, 0)) as reportingsku_idx,

    cast(substring(ispromosku, 1, 255) as text(255)) as ispromosku,

    cast(substring(iscannibsku, 1, 255) as text(255)) as iscannibsku,

    cast(substring(issi_prepromoday, 1, 255) as text(255)) as issi_prepromoday,

    cast(substring(issi_onpromoday, 1, 255) as text(255)) as issi_onpromoday,

    cast(substring(issi_postpromoday, 1, 255) as text(255)) as issi_postpromoday,

    cast(substring(isso_prepromoday, 1, 255) as text(255)) as isso_prepromoday,

    cast(substring(isso_onpromoday, 1, 255) as text(255)) as isso_onpromoday,

    cast(substring(isso_postpromoday, 1, 255) as text(255)) as isso_postpromoday,

    cast(scen_idx as number(38, 0)) as scen_idx,

    cast(substring(scen_name, 1, 255) as text(255)) as scen_name,

    cast(substring(scen_code, 1, 255) as text(255)) as scen_code,

    cast(scenario_guid as text(255)) as scenario_guid,

    cast(substring(frozen_forecast, 1, 255) as text(255)) as frozen_forecast,

    cast(si_b_vol_cse as number(38, 10)) as si_b_vol_cse,

    cast(si_b_vol_sgl as number(38, 10)) as si_b_vol_sgl,

    cast(si_a_vol_cse as number(38, 10)) as si_a_vol_cse,

    cast(si_a_vol_sgl as number(38, 10)) as si_a_vol_sgl,

    cast(si_t_vol_cse as number(38, 10)) as si_t_vol_cse,

    cast(si_t_vol_sgl as number(38, 10)) as si_t_vol_sgl,

    cast(si_m_vol_cse as number(38, 10)) as si_m_vol_cse,

    cast(si_m_vol_sgl as number(38, 10)) as si_m_vol_sgl,

    cast(si_i_vol_cse as number(38, 10)) as si_i_vol_cse,

    cast(si_i_vol_sgl as number(38, 10)) as si_i_vol_sgl,

    cast(so_b_vol_cse as number(38, 10)) as so_b_vol_cse,

    cast(so_b_vol_sgl as number(38, 10)) as so_b_vol_sgl,

    cast(so_a_vol_cse as number(38, 10)) as so_a_vol_cse,

    cast(so_a_vol_sgl as number(38, 10)) as so_a_vol_sgl,

    cast(so_t_vol_cse as number(38, 10)) as so_t_vol_cse,

    cast(so_t_vol_sgl as number(38, 10)) as so_t_vol_sgl,

    cast(so_m_vol_cse as number(38, 10)) as so_m_vol_cse,

    cast(so_m_vol_sgl as number(38, 10)) as so_m_vol_sgl,

    cast(so_i_vol_cse as number(38, 10)) as so_i_vol_cse,

    cast(so_i_vol_sgl as number(38, 10)) as so_i_vol_sgl,

    cast(si_cannib_vol_cse as number(38, 10)) as si_cannib_vol_cse,

    cast(si_cannib_vol_sgl as number(38, 10)) as si_cannib_vol_sgl,

    cast(so_cannib_vol_cse as number(38, 10)) as so_cannib_vol_cse,

    cast(so_cannib_vol_sgl as number(38, 10)) as so_cannib_vol_sgl,

    cast(si_cannib_basevol_cse as number(38, 10)) as si_cannib_basevol_cse,

    cast(si_cannib_basevol_sgl as number(38, 10)) as si_cannib_basevol_sgl,

    cast(so_cannib_basevol_cse as number(38, 10)) as so_cannib_basevol_cse,

    cast(so_cannib_basevol_sgl as number(38, 10)) as so_cannib_basevol_sgl,

    cast(si_cannib_loss_vol_cse as number(38, 10)) as si_cannib_loss_vol_cse,

    cast(si_cannib_loss_vol_sgl as number(38, 10)) as si_cannib_loss_vol_sgl,

    cast(so_cannib_loss_vol_cse as number(38, 10)) as so_cannib_loss_vol_cse,

    cast(so_cannib_loss_vol_sgl as number(38, 10)) as so_cannib_loss_vol_sgl,

    cast(si_predip_vol_cse as number(38, 10)) as si_predip_vol_cse,

    cast(si_predip_vol_sgl as number(38, 10)) as si_predip_vol_sgl,

    cast(si_postdip_vol_cse as number(38, 10)) as si_postdip_vol_cse,

    cast(si_postdip_vol_sgl as number(38, 10)) as si_postdip_vol_sgl,

    cast(so_predip_vol_cse as number(38, 10)) as so_predip_vol_cse,

    cast(so_predip_vol_sgl as number(38, 10)) as so_predip_vol_sgl,

    cast(so_postdip_vol_cse as number(38, 10)) as so_postdip_vol_cse,

    cast(so_postdip_vol_sgl as number(38, 10)) as so_postdip_vol_sgl,

    cast(si_predip_basevol_cse as number(38, 10)) as si_predip_basevol_cse,

    cast(si_predip_basevol_sgl as number(38, 10)) as si_predip_basevol_sgl,

    cast(si_postdip_basevol_cse as number(38, 10)) as si_postdip_basevol_cse,

    cast(si_postdip_basevol_sgl as number(38, 10)) as si_postdip_basevol_sgl,

    cast(so_predip_basevol_cse as number(38, 10)) as so_predip_basevol_cse,

    cast(so_predip_basevol_sgl as number(38, 10)) as so_predip_basevol_sgl,

    cast(so_postdip_basevol_cse as number(38, 10)) as so_postdip_basevol_cse,

    cast(so_postdip_basevol_sgl as number(38, 10)) as so_postdip_basevol_sgl,

    cast(postpromodippercent_si as number(38, 10)) as postpromodippercent_si,

    cast(postpromodippercent_so as number(38, 10)) as postpromodippercent_so,

    cast(prepromodippercent_si as number(38, 10)) as prepromodippercent_si,

    cast(prepromodippercent_so as number(38, 10)) as prepromodippercent_so,

    cast(onpromophasingpercent_si as number(38, 10)) as onpromophasingpercent_si,

    cast(onpromophasingpercent_so as number(38, 10)) as onpromophasingpercent_so,

    cast(robfundingrequired as number(38, 10)) as robfundingrequired,

    cast(actuals_tot_vol_kg as number(38, 10)) as actuals_tot_vol_kg,

    cast(
        actuals_ap_gross_sales_value as number(38, 10)
    ) as actuals_ap_gross_sales_value,

    cast(
        actuals_ap_range_support_allowance as number(38, 10)
    ) as actuals_ap_range_support_allowance,

    cast(
        actuals_ap_everyday_low_prices as number(38, 10)
    ) as actuals_ap_everyday_low_prices,

    cast(actuals_ap_permanent_disc as number(38, 10)) as actuals_ap_permanent_disc,

    cast(actuals_ap_off_invoice_disc as number(38, 10)) as actuals_ap_off_invoice_disc,

    cast(
        actuals_ap_invoiced_sales_value as number(38, 10)
    ) as actuals_ap_invoiced_sales_value,

    cast(
        actuals_ap_early_settlement_disc as number(38, 10)
    ) as actuals_ap_early_settlement_disc,

    cast(
        actuals_ap_growth_incentives as number(38, 10)
    ) as actuals_ap_growth_incentives,

    cast(actuals_ap_net_sales_value as number(38, 10)) as actuals_ap_net_sales_value,

    cast(actuals_ap_retro as number(38, 10)) as actuals_ap_retro,

    cast(actuals_ap_avp_disc as number(38, 10)) as actuals_ap_avp_disc,

    cast(actuals_ap_variable_trade as number(38, 10)) as actuals_ap_variable_trade,

    cast(
        actuals_ap_promo_fixed_funding as number(38, 10)
    ) as actuals_ap_promo_fixed_funding,

    cast(
        actuals_ap_range_support_incentives as number(38, 10)
    ) as actuals_ap_range_support_incentives,

    cast(
        actuals_ap_net_net_sales_value as number(38, 10)
    ) as actuals_ap_net_net_sales_value,

    cast(
        actuals_ap_direct_shopper_marketing as number(38, 10)
    ) as actuals_ap_direct_shopper_marketing,

    cast(
        actuals_ap_other_direct_payments as number(38, 10)
    ) as actuals_ap_other_direct_payments,

    cast(
        actuals_ap_indirect_shopper_marketing as number(38, 10)
    ) as actuals_ap_indirect_shopper_marketing,

    cast(
        actuals_ap_other_indirect_payments as number(38, 10)
    ) as actuals_ap_other_indirect_payments,

    cast(
        actuals_ap_fixed_trade_cust_invoiced as number(38, 10)
    ) as actuals_ap_fixed_trade_cust_invoiced,

    cast(
        actuals_ap_total_trade_cust_invoiced as number(38, 10)
    ) as actuals_ap_total_trade_cust_invoiced,

    cast(
        actuals_ap_fixed_trade_non_cust_invoiced as number(38, 10)
    ) as actuals_ap_fixed_trade_non_cust_invoiced,

    cast(actuals_ap_total_trade as number(38, 10)) as actuals_ap_total_trade,

    cast(
        actuals_ap_net_realisable_revenue as number(38, 10)
    ) as actuals_ap_net_realisable_revenue,

    cast(
        actuals_ap_tot_prime_cost_standard as number(38, 10)
    ) as actuals_ap_tot_prime_cost_standard,

    cast(
        actuals_ap_gross_margin_standard as number(38, 10)
    ) as actuals_ap_gross_margin_standard,

    cast(actuals_ap_gcat_standard as number(38, 10)) as actuals_ap_gcat_standard,

    cast(actuals_manso_tot_vol_kg as number(38, 10)) as actuals_manso_tot_vol_kg,

    cast(
        actuals_manso_gross_sales_value as number(38, 10)
    ) as actuals_manso_gross_sales_value,

    cast(
        actuals_manso_range_support_allowance as number(38, 10)
    ) as actuals_manso_range_support_allowance,

    cast(
        actuals_manso_everyday_low_prices as number(38, 10)
    ) as actuals_manso_everyday_low_prices,

    cast(
        actuals_manso_permanent_disc as number(38, 10)
    ) as actuals_manso_permanent_disc,

    cast(
        actuals_manso_off_invoice_disc as number(38, 10)
    ) as actuals_manso_off_invoice_disc,

    cast(
        actuals_manso_invoiced_sales_value as number(38, 10)
    ) as actuals_manso_invoiced_sales_value,

    cast(
        actuals_manso_early_settlement_disc as number(38, 10)
    ) as actuals_manso_early_settlement_disc,

    cast(
        actuals_manso_growth_incentives as number(38, 10)
    ) as actuals_manso_growth_incentives,

    cast(
        actuals_manso_net_sales_value as number(38, 10)
    ) as actuals_manso_net_sales_value,

    cast(actuals_manso_retro as number(38, 10)) as actuals_manso_retro,

    cast(actuals_manso_avp_disc as number(38, 10)) as actuals_manso_avp_disc,

    cast(
        actuals_manso_variable_trade as number(38, 10)
    ) as actuals_manso_variable_trade,

    cast(
        actuals_manso_promo_fixed_funding as number(38, 10)
    ) as actuals_manso_promo_fixed_funding,

    cast(
        actuals_manso_range_support_incentives as number(38, 10)
    ) as actuals_manso_range_support_incentives,

    cast(
        actuals_manso_net_net_sales_value as number(38, 10)
    ) as actuals_manso_net_net_sales_value,

    cast(
        actuals_manso_direct_shopper_marketing as number(38, 10)
    ) as actuals_manso_direct_shopper_marketing,

    cast(
        actuals_manso_other_direct_payments as number(38, 10)
    ) as actuals_manso_other_direct_payments,

    cast(
        actuals_manso_indirect_shopper_marketing as number(38, 10)
    ) as actuals_manso_indirect_shopper_marketing,

    cast(
        actuals_manso_other_indirect_payments as number(38, 10)
    ) as actuals_manso_other_indirect_payments,

    cast(
        actuals_manso_fixed_trade_cust_invoiced as number(38, 10)
    ) as actuals_manso_fixed_trade_cust_invoiced,

    cast(
        actuals_manso_total_trade_cust_invoiced as number(38, 10)
    ) as actuals_manso_total_trade_cust_invoiced,

    cast(
        actuals_manso_fixed_trade_non_cust_invoiced as number(38, 10)
    ) as actuals_manso_fixed_trade_non_cust_invoiced,

    cast(actuals_manso_total_trade as number(38, 10)) as actuals_manso_total_trade,

    cast(
        actuals_manso_net_realisable_revenue as number(38, 10)
    ) as actuals_manso_net_realisable_revenue,

    cast(
        actuals_manso_tot_prime_cost_standard as number(38, 10)
    ) as actuals_manso_tot_prime_cost_standard,

    cast(
        actuals_manso_gross_margin_standard as number(38, 10)
    ) as actuals_manso_gross_margin_standard,

    cast(actuals_manso_gcat_standard as number(38, 10)) as actuals_manso_gcat_standard,

    cast(actuals_retail_tot_vol_kg as number(38, 10)) as actuals_retail_tot_vol_kg,

    cast(
        actuals_ap_retail_revenue_mrrsp as number(38, 10)
    ) as actuals_ap_retail_revenue_mrrsp,

    cast(
        actuals_ap_retail_revenue_rsp as number(38, 10)
    ) as actuals_ap_retail_revenue_rsp,

    cast(
        actuals_ap_retail_revenue_net as number(38, 10)
    ) as actuals_ap_retail_revenue_net,

    cast(
        actuals_ap_retail_cost_of_sales as number(38, 10)
    ) as actuals_ap_retail_cost_of_sales,

    cast(
        actuals_ap_retail_retailer_retro_funding as number(38, 10)
    ) as actuals_ap_retail_retailer_retro_funding,

    cast(
        actuals_ap_retail_margin_excl_fixed_funding as number(38, 10)
    ) as actuals_ap_retail_margin_excl_fixed_funding,

    cast(
        actuals_ap_retail_promo_fixed_spend as number(38, 10)
    ) as actuals_ap_retail_promo_fixed_spend,

    cast(
        actuals_ap_retail_total_spend as number(38, 10)
    ) as actuals_ap_retail_total_spend,

    cast(
        actuals_ap_retail_margin_incl_fixed_funding as number(38, 10)
    ) as actuals_ap_retail_margin_incl_fixed_funding,

    cast(
        actuals_ap_retail_revenue_net_excl_mrrsp as number(38, 10)
    ) as actuals_ap_retail_revenue_net_excl_mrrsp,

    cast(
        actuals_ap_retail_revenue_net_excl_rsp as number(38, 10)
    ) as actuals_ap_retail_revenue_net_excl_rsp,

    cast(base_tot_vol_kg as number(38, 10)) as base_tot_vol_kg,

    cast(base_ap_gross_sales_value as number(38, 10)) as base_ap_gross_sales_value,

    cast(
        base_ap_range_support_allowance as number(38, 10)
    ) as base_ap_range_support_allowance,

    cast(base_ap_everyday_low_prices as number(38, 10)) as base_ap_everyday_low_prices,

    cast(base_ap_permanent_disc as number(38, 10)) as base_ap_permanent_disc,

    cast(base_ap_off_invoice_disc as number(38, 10)) as base_ap_off_invoice_disc,

    cast(
        base_ap_invoiced_sales_value as number(38, 10)
    ) as base_ap_invoiced_sales_value,

    cast(
        base_ap_early_settlement_disc as number(38, 10)
    ) as base_ap_early_settlement_disc,

    cast(base_ap_growth_incentives as number(38, 10)) as base_ap_growth_incentives,

    cast(base_ap_net_sales_value as number(38, 10)) as base_ap_net_sales_value,

    cast(base_ap_retro as number(38, 10)) as base_ap_retro,

    cast(base_ap_avp_disc as number(38, 10)) as base_ap_avp_disc,

    cast(base_ap_variable_trade as number(38, 10)) as base_ap_variable_trade,

    cast(base_ap_promo_fixed_funding as number(38, 10)) as base_ap_promo_fixed_funding,

    cast(
        base_ap_range_support_incentives as number(38, 10)
    ) as base_ap_range_support_incentives,

    cast(base_ap_net_net_sales_value as number(38, 10)) as base_ap_net_net_sales_value,

    cast(
        base_ap_direct_shopper_marketing as number(38, 10)
    ) as base_ap_direct_shopper_marketing,

    cast(
        base_ap_other_direct_payments as number(38, 10)
    ) as base_ap_other_direct_payments,

    cast(
        base_ap_indirect_shopper_marketing as number(38, 10)
    ) as base_ap_indirect_shopper_marketing,

    cast(
        base_ap_other_indirect_payments as number(38, 10)
    ) as base_ap_other_indirect_payments,

    cast(
        base_ap_fixed_trade_cust_invoiced as number(38, 10)
    ) as base_ap_fixed_trade_cust_invoiced,

    cast(
        base_ap_total_trade_cust_invoiced as number(38, 10)
    ) as base_ap_total_trade_cust_invoiced,

    cast(
        base_ap_fixed_trade_non_cust_invoiced as number(38, 10)
    ) as base_ap_fixed_trade_non_cust_invoiced,

    cast(base_ap_total_trade as number(38, 10)) as base_ap_total_trade,

    cast(
        base_ap_net_realisable_revenue as number(38, 10)
    ) as base_ap_net_realisable_revenue,

    cast(
        base_ap_tot_prime_cost_standard as number(38, 10)
    ) as base_ap_tot_prime_cost_standard,

    cast(
        base_ap_gross_margin_standard as number(38, 10)
    ) as base_ap_gross_margin_standard,

    cast(base_ap_gcat_standard as number(38, 10)) as base_ap_gcat_standard,

    cast(base_manso_tot_vol_kg as number(38, 10)) as base_manso_tot_vol_kg,

    cast(
        base_manso_gross_sales_value as number(38, 10)
    ) as base_manso_gross_sales_value,

    cast(
        base_manso_range_support_allowance as number(38, 10)
    ) as base_manso_range_support_allowance,

    cast(
        base_manso_everyday_low_prices as number(38, 10)
    ) as base_manso_everyday_low_prices,

    cast(base_manso_permanent_disc as number(38, 10)) as base_manso_permanent_disc,

    cast(base_manso_off_invoice_disc as number(38, 10)) as base_manso_off_invoice_disc,

    cast(
        base_manso_invoiced_sales_value as number(38, 10)
    ) as base_manso_invoiced_sales_value,

    cast(
        base_manso_early_settlement_disc as number(38, 10)
    ) as base_manso_early_settlement_disc,

    cast(
        base_manso_growth_incentives as number(38, 10)
    ) as base_manso_growth_incentives,

    cast(base_manso_net_sales_value as number(38, 10)) as base_manso_net_sales_value,

    cast(base_manso_retro as number(38, 10)) as base_manso_retro,

    cast(base_manso_avp_disc as number(38, 10)) as base_manso_avp_disc,

    cast(base_manso_variable_trade as number(38, 10)) as base_manso_variable_trade,

    cast(
        base_manso_promo_fixed_funding as number(38, 10)
    ) as base_manso_promo_fixed_funding,

    cast(
        base_manso_range_support_incentives as number(38, 10)
    ) as base_manso_range_support_incentives,

    cast(
        base_manso_net_net_sales_value as number(38, 10)
    ) as base_manso_net_net_sales_value,

    cast(
        base_manso_direct_shopper_marketing as number(38, 10)
    ) as base_manso_direct_shopper_marketing,

    cast(
        base_manso_other_direct_payments as number(38, 10)
    ) as base_manso_other_direct_payments,

    cast(
        base_manso_indirect_shopper_marketing as number(38, 10)
    ) as base_manso_indirect_shopper_marketing,

    cast(
        base_manso_other_indirect_payments as number(38, 10)
    ) as base_manso_other_indirect_payments,

    cast(
        base_manso_fixed_trade_cust_invoiced as number(38, 10)
    ) as base_manso_fixed_trade_cust_invoiced,

    cast(
        base_manso_total_trade_cust_invoiced as number(38, 10)
    ) as base_manso_total_trade_cust_invoiced,

    cast(
        base_manso_fixed_trade_non_cust_invoiced as number(38, 10)
    ) as base_manso_fixed_trade_non_cust_invoiced,

    cast(base_manso_total_trade as number(38, 10)) as base_manso_total_trade,

    cast(
        base_manso_net_realisable_revenue as number(38, 10)
    ) as base_manso_net_realisable_revenue,

    cast(
        base_manso_tot_prime_cost_standard as number(38, 10)
    ) as base_manso_tot_prime_cost_standard,

    cast(
        base_manso_gross_margin_standard as number(38, 10)
    ) as base_manso_gross_margin_standard,

    cast(base_manso_gcat_standard as number(38, 10)) as base_manso_gcat_standard,

    cast(base_retail_tot_vol_kg as number(38, 10)) as base_retail_tot_vol_kg,

    cast(
        base_ap_retail_revenue_mrrsp as number(38, 10)
    ) as base_ap_retail_revenue_mrrsp,

    cast(base_ap_retail_revenue_rsp as number(38, 10)) as base_ap_retail_revenue_rsp,

    cast(base_ap_retail_revenue_net as number(38, 10)) as base_ap_retail_revenue_net,

    cast(
        base_ap_retail_cost_of_sales as number(38, 10)
    ) as base_ap_retail_cost_of_sales,

    cast(
        base_ap_retail_retailer_retro_funding as number(38, 10)
    ) as base_ap_retail_retailer_retro_funding,

    cast(
        base_ap_retail_margin_excl_fixed_funding as number(38, 10)
    ) as base_ap_retail_margin_excl_fixed_funding,

    cast(
        base_ap_retail_promo_fixed_spend as number(38, 10)
    ) as base_ap_retail_promo_fixed_spend,

    cast(base_ap_retail_total_spend as number(38, 10)) as base_ap_retail_total_spend,

    cast(
        base_ap_retail_margin_incl_fixed_funding as number(38, 10)
    ) as base_ap_retail_margin_incl_fixed_funding,

    cast(
        base_ap_retail_revenue_net_excl_mrrsp as number(38, 10)
    ) as base_ap_retail_revenue_net_excl_mrrsp,

    cast(
        base_ap_retail_revenue_net_excl_rsp as number(38, 10)
    ) as base_ap_retail_revenue_net_excl_rsp,

    cast(forecast_tot_vol_kg as number(38, 10)) as forecast_tot_vol_kg,

    cast(
        forecast_ap_gross_sales_value as number(38, 10)
    ) as forecast_ap_gross_sales_value,

    cast(
        forecast_ap_range_support_allowance as number(38, 10)
    ) as forecast_ap_range_support_allowance,

    cast(
        forecast_ap_everyday_low_prices as number(38, 10)
    ) as forecast_ap_everyday_low_prices,

    cast(forecast_ap_permanent_disc as number(38, 10)) as forecast_ap_permanent_disc,

    cast(
        forecast_ap_off_invoice_disc as number(38, 10)
    ) as forecast_ap_off_invoice_disc,

    cast(
        forecast_ap_invoiced_sales_value as number(38, 10)
    ) as forecast_ap_invoiced_sales_value,

    cast(
        forecast_ap_early_settlement_disc as number(38, 10)
    ) as forecast_ap_early_settlement_disc,

    cast(
        forecast_ap_growth_incentives as number(38, 10)
    ) as forecast_ap_growth_incentives,

    cast(forecast_ap_net_sales_value as number(38, 10)) as forecast_ap_net_sales_value,

    cast(forecast_ap_retro as number(38, 10)) as forecast_ap_retro,

    cast(forecast_ap_avp_disc as number(38, 10)) as forecast_ap_avp_disc,

    cast(forecast_ap_variable_trade as number(38, 10)) as forecast_ap_variable_trade,

    cast(
        forecast_ap_promo_fixed_funding as number(38, 10)
    ) as forecast_ap_promo_fixed_funding,

    cast(
        forecast_ap_range_support_incentives as number(38, 10)
    ) as forecast_ap_range_support_incentives,

    cast(
        forecast_ap_net_net_sales_value as number(38, 10)
    ) as forecast_ap_net_net_sales_value,

    cast(
        forecast_ap_direct_shopper_marketing as number(38, 10)
    ) as forecast_ap_direct_shopper_marketing,

    cast(
        forecast_ap_other_direct_payments as number(38, 10)
    ) as forecast_ap_other_direct_payments,

    cast(
        forecast_ap_indirect_shopper_marketing as number(38, 10)
    ) as forecast_ap_indirect_shopper_marketing,

    cast(
        forecast_ap_other_indirect_payments as number(38, 10)
    ) as forecast_ap_other_indirect_payments,

    cast(
        forecast_ap_fixed_trade_cust_invoiced as number(38, 10)
    ) as forecast_ap_fixed_trade_cust_invoiced,

    cast(
        forecast_ap_total_trade_cust_invoiced as number(38, 10)
    ) as forecast_ap_total_trade_cust_invoiced,

    cast(
        forecast_ap_fixed_trade_non_cust_invoiced as number(38, 10)
    ) as forecast_ap_fixed_trade_non_cust_invoiced,

    cast(forecast_ap_total_trade as number(38, 10)) as forecast_ap_total_trade,

    cast(forecast_ap_total_trade_gbp as number(38, 10)) as forecast_ap_total_trade_gbp,

    cast(
        forecast_ap_net_realisable_revenue as number(38, 10)
    ) as forecast_ap_net_realisable_revenue,

    cast(
        forecast_ap_tot_prime_cost_standard as number(38, 10)
    ) as forecast_ap_tot_prime_cost_standard,

    cast(
        forecast_ap_gross_margin_standard as number(38, 10)
    ) as forecast_ap_gross_margin_standard,

    cast(forecast_ap_gcat_standard as number(38, 10)) as forecast_ap_gcat_standard,

    cast(forecast_manso_tot_vol_kg as number(38, 10)) as forecast_manso_tot_vol_kg,

    cast(
        forecast_manso_gross_sales_value as number(38, 10)
    ) as forecast_manso_gross_sales_value,

    cast(
        forecast_manso_range_support_allowance as number(38, 10)
    ) as forecast_manso_range_support_allowance,

    cast(
        forecast_manso_everyday_low_prices as number(38, 10)
    ) as forecast_manso_everyday_low_prices,

    cast(
        forecast_manso_permanent_disc as number(38, 10)
    ) as forecast_manso_permanent_disc,

    cast(
        forecast_manso_off_invoice_disc as number(38, 10)
    ) as forecast_manso_off_invoice_disc,

    cast(
        forecast_manso_invoiced_sales_value as number(38, 10)
    ) as forecast_manso_invoiced_sales_value,

    cast(
        forecast_manso_early_settlement_disc as number(38, 10)
    ) as forecast_manso_early_settlement_disc,

    cast(
        forecast_manso_growth_incentives as number(38, 10)
    ) as forecast_manso_growth_incentives,

    cast(
        forecast_manso_net_sales_value as number(38, 10)
    ) as forecast_manso_net_sales_value,

    cast(forecast_manso_retro as number(38, 10)) as forecast_manso_retro,

    cast(forecast_manso_avp_disc as number(38, 10)) as forecast_manso_avp_disc,

    cast(
        forecast_manso_variable_trade as number(38, 10)
    ) as forecast_manso_variable_trade,

    cast(
        forecast_manso_promo_fixed_funding as number(38, 10)
    ) as forecast_manso_promo_fixed_funding,

    cast(
        forecast_manso_range_support_incentives as number(38, 10)
    ) as forecast_manso_range_support_incentives,

    cast(
        forecast_manso_net_net_sales_value as number(38, 10)
    ) as forecast_manso_net_net_sales_value,

    cast(
        forecast_manso_direct_shopper_marketing as number(38, 10)
    ) as forecast_manso_direct_shopper_marketing,

    cast(
        forecast_manso_other_direct_payments as number(38, 10)
    ) as forecast_manso_other_direct_payments,

    cast(
        forecast_manso_indirect_shopper_marketing as number(38, 10)
    ) as forecast_manso_indirect_shopper_marketing,

    cast(
        forecast_manso_other_indirect_payments as number(38, 10)
    ) as forecast_manso_other_indirect_payments,

    cast(
        forecast_manso_fixed_trade_cust_invoiced as number(38, 10)
    ) as forecast_manso_fixed_trade_cust_invoiced,

    cast(
        forecast_manso_total_trade_cust_invoiced as number(38, 10)
    ) as forecast_manso_total_trade_cust_invoiced,

    cast(
        forecast_manso_fixed_trade_non_cust_invoiced as number(38, 10)
    ) as forecast_manso_fixed_trade_non_cust_invoiced,

    cast(forecast_manso_total_trade as number(38, 10)) as forecast_manso_total_trade,

    cast(
        forecast_manso_net_realisable_revenue as number(38, 10)
    ) as forecast_manso_net_realisable_revenue,

    cast(
        forecast_manso_tot_prime_cost_standard as number(38, 10)
    ) as forecast_manso_tot_prime_cost_standard,

    cast(
        forecast_manso_gross_margin_standard as number(38, 10)
    ) as forecast_manso_gross_margin_standard,

    cast(
        forecast_manso_gcat_standard as number(38, 10)
    ) as forecast_manso_gcat_standard,

    cast(forecast_retail_tot_vol_kg as number(38, 10)) as forecast_retail_tot_vol_kg,

    cast(
        forecast_ap_retail_revenue_mrrsp as number(38, 10)
    ) as forecast_ap_retail_revenue_mrrsp,

    cast(
        forecast_ap_retail_revenue_rsp as number(38, 10)
    ) as forecast_ap_retail_revenue_rsp,

    cast(
        forecast_ap_retail_revenue_net as number(38, 10)
    ) as forecast_ap_retail_revenue_net,

    cast(
        forecast_ap_retail_cost_of_sales as number(38, 10)
    ) as forecast_ap_retail_cost_of_sales,

    cast(
        forecast_ap_retail_retailer_retro_funding as number(38, 10)
    ) as forecast_ap_retail_retailer_retro_funding,

    cast(
        forecast_ap_retail_margin_excl_fixed_funding as number(38, 10)
    ) as forecast_ap_retail_margin_excl_fixed_funding,

    cast(
        forecast_ap_retail_promo_fixed_spend as number(38, 10)
    ) as forecast_ap_retail_promo_fixed_spend,

    cast(
        forecast_ap_retail_total_spend as number(38, 10)
    ) as forecast_ap_retail_total_spend,

    cast(
        forecast_ap_retail_margin_incl_fixed_funding as number(38, 10)
    ) as forecast_ap_retail_margin_incl_fixed_funding,

    cast(
        forecast_ap_retail_revenue_net_excl_mrrsp as number(38, 10)
    ) as forecast_ap_retail_revenue_net_excl_mrrsp,

    cast(
        forecast_ap_retail_revenue_net_excl_rsp as number(38, 10)
    ) as forecast_ap_retail_revenue_net_excl_rsp,

    cast(si_b_vol_kg as number(38, 10)) as si_b_vol_kg,

    cast(si_a_vol_kg as number(38, 10)) as si_a_vol_kg,

    cast(si_t_vol_kg as number(38, 10)) as si_t_vol_kg,

    cast(si_m_vol_kg as number(38, 10)) as si_m_vol_kg,

    cast(si_i_vol_kg as number(38, 10)) as si_i_vol_kg,

    cast(so_b_vol_kg as number(38, 10)) as so_b_vol_kg,

    cast(so_a_vol_kg as number(38, 10)) as so_a_vol_kg,

    cast(so_t_vol_kg as number(38, 10)) as so_t_vol_kg,

    cast(so_m_vol_kg as number(38, 10)) as so_m_vol_kg,

    cast(so_i_vol_kg as number(38, 10)) as so_i_vol_kg,

    cast(si_cannib_vol_kg as number(38, 10)) as si_cannib_vol_kg,

    cast(so_cannib_vol_kg as number(38, 10)) as so_cannib_vol_kg,

    cast(si_cannib_basevol_kg as number(38, 10)) as si_cannib_basevol_kg,

    cast(so_cannib_basevol_kg as number(38, 10)) as so_cannib_basevol_kg,

    cast(si_cannib_loss_vol_kg as number(38, 10)) as si_cannib_loss_vol_kg,

    cast(so_cannib_loss_vol_kg as number(38, 10)) as so_cannib_loss_vol_kg,

    cast(si_predip_vol_kg as number(38, 10)) as si_predip_vol_kg,

    cast(si_postdip_vol_kg as number(38, 10)) as si_postdip_vol_kg,

    cast(so_predip_vol_kg as number(38, 10)) as so_predip_vol_kg,

    cast(so_postdip_vol_kg as number(38, 10)) as so_postdip_vol_kg,

    cast(si_predip_basevol_kg as number(38, 10)) as si_predip_basevol_kg,

    cast(si_postdip_basevol_kg as number(38, 10)) as si_postdip_basevol_kg,

    cast(so_predip_basevol_kg as number(38, 10)) as so_predip_basevol_kg,

    cast(so_postdip_basevol_kg as number(38, 10)) as so_postdip_basevol_kg,

    cast(actuals_tot_vol_ca as number(38, 10)) as actuals_tot_vol_ca,

    cast(actuals_tot_vol_ul as number(38, 10)) as actuals_tot_vol_ul,

    cast(base_tot_vol_ca as number(38, 10)) as base_tot_vol_ca,

    cast(base_tot_vol_ul as number(38, 10)) as base_tot_vol_ul,

    cast(forecast_tot_vol_ca as number(38, 10)) as forecast_tot_vol_ca,

    cast(forecast_tot_vol_ul as number(38, 10)) as forecast_tot_vol_ul,

    cast(v_ca_kg_conv as number(38, 10)) as v_ca_kg_conv,

    cast(
        substring(prm_rpt_customer_code, 1, 255) as text(255)
    ) as prm_rpt_customer_code,

    cast(prm_rpt_customer_guid as text(255)) as prm_rpt_customer_guid,

    cast(substring(promo_code, 1, 50) as text(50)) as promo_code,

    cast(unique_key as text(255)) as unique_key
from renamed
