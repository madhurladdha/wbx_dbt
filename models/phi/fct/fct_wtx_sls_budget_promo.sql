{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        alias='fct_wtx_sls_budget_promo_exc',
        enabled=false,
        tags=["sales_budget","budget", "promotion","sls_budget_promo"],
        unique_key="unique_key",
        on_schema_change="sync_all_columns",
        full_refresh=false,
        pre_hook="""
                    {% if check_table_exists( this.schema, this.table ) == 'True' %}
                    DELETE FROM {{ this }} 
                    WHERE SNAPSHOT_DATE in
                    (SELECT SNAPSHOT_DATE FROM {{ ref('int_f_wtx_sls_budget_promo') }});
                    {% endif %}  
                    {% if check_table_exists( this.schema, this.table ) == 'True' %}
                    DELETE FROM {{ this }} 
                    WHERE FROZEN_FORECAST in
                    (SELECT FROZEN_FORECAST FROM {{ ref('int_f_wtx_sls_budget_promo') }})
                    {% endif %} 
                    """,
        snowflake_warehouse=env_var("DBT_WBX_SF_WH")
    )
}}

/*  This is an alternative to the other model called by the same name.  This one has been assigned an alias to ensure it would materialize under 
    a different name.
    THIS MODEL IS NOT PLANNED TO EVER BE USED.  IT WAS NEVER RUN IN IICS AND SHOULD NOT RUN.
*/
with
    old_table as (
        select *
        from {{ ref("conv_wtx_sls_budget_promo_fact") }}
        {% if check_table_exists(this.schema, this.table) == "False" %}
        limit {{ env_var("DBT_NO_LIMIT") }}  -- --------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
        {% else %} limit {{ env_var("DBT_LIMIT") }}  -- ---Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist
        {% endif %}

    ),
    new_table as (
        select *
        from {{ ref("int_f_wtx_sls_budget_promo") }}
        {% if check_table_exists(this.schema, this.table) == "True" %}
        limit {{ env_var("DBT_NO_LIMIT") }}
        {% else %} limit {{ env_var("DBT_LIMIT") }}
        {% endif %}
    ),
    old_fct as (select * from old_table),
    new_fct as (
        select
            cast(substring(source_system, 1, 255) as text(255)) as source_system,

            cast(promo_idx as number(38, 0)) as promo_idx,

            cast(promo_guid as text(255)) as promo_guid,

            cast(cust_idx as number(38, 0)) as cust_idx,

            cast(
                substring(plan_source_customer_code, 1, 255) as text(255)
            ) as plan_source_customer_code,

            cast(
                customer_address_number_guid as text(255)
            ) as customer_address_number_guid,

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

            cast(
                substring(issi_postpromoday, 1, 255) as text(255)
            ) as issi_postpromoday,

            cast(substring(isso_prepromoday, 1, 255) as text(255)) as isso_prepromoday,

            cast(substring(isso_onpromoday, 1, 255) as text(255)) as isso_onpromoday,

            cast(
                substring(isso_postpromoday, 1, 255) as text(255)
            ) as isso_postpromoday,

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

            cast(
                onpromophasingpercent_si as number(38, 10)
            ) as onpromophasingpercent_si,

            cast(
                onpromophasingpercent_so as number(38, 10)
            ) as onpromophasingpercent_so,

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

            cast(
                actuals_ap_permanent_disc as number(38, 10)
            ) as actuals_ap_permanent_disc,

            cast(
                actuals_ap_off_invoice_disc as number(38, 10)
            ) as actuals_ap_off_invoice_disc,

            cast(
                actuals_ap_invoiced_sales_value as number(38, 10)
            ) as actuals_ap_invoiced_sales_value,

            cast(
                actuals_ap_early_settlement_disc as number(38, 10)
            ) as actuals_ap_early_settlement_disc,

            cast(
                actuals_ap_growth_incentives as number(38, 10)
            ) as actuals_ap_growth_incentives,

            cast(
                actuals_ap_net_sales_value as number(38, 10)
            ) as actuals_ap_net_sales_value,

            cast(actuals_ap_retro as number(38, 10)) as actuals_ap_retro,

            cast(actuals_ap_avp_disc as number(38, 10)) as actuals_ap_avp_disc,

            cast(
                actuals_ap_variable_trade as number(38, 10)
            ) as actuals_ap_variable_trade,

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

            cast(
                actuals_ap_gcat_standard as number(38, 10)
            ) as actuals_ap_gcat_standard,

            cast(
                actuals_manso_tot_vol_kg as number(38, 10)
            ) as actuals_manso_tot_vol_kg,

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

            cast(
                actuals_manso_total_trade as number(38, 10)
            ) as actuals_manso_total_trade,

            cast(
                actuals_manso_net_realisable_revenue as number(38, 10)
            ) as actuals_manso_net_realisable_revenue,

            cast(
                actuals_manso_tot_prime_cost_standard as number(38, 10)
            ) as actuals_manso_tot_prime_cost_standard,

            cast(
                actuals_manso_gross_margin_standard as number(38, 10)
            ) as actuals_manso_gross_margin_standard,

            cast(
                actuals_manso_gcat_standard as number(38, 10)
            ) as actuals_manso_gcat_standard,

            cast(
                actuals_retail_tot_vol_kg as number(38, 10)
            ) as actuals_retail_tot_vol_kg,

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

            cast(
                base_ap_gross_sales_value as number(38, 10)
            ) as base_ap_gross_sales_value,

            cast(
                base_ap_range_support_allowance as number(38, 10)
            ) as base_ap_range_support_allowance,

            cast(
                base_ap_everyday_low_prices as number(38, 10)
            ) as base_ap_everyday_low_prices,

            cast(base_ap_permanent_disc as number(38, 10)) as base_ap_permanent_disc,

            cast(
                base_ap_off_invoice_disc as number(38, 10)
            ) as base_ap_off_invoice_disc,

            cast(
                base_ap_invoiced_sales_value as number(38, 10)
            ) as base_ap_invoiced_sales_value,

            cast(
                base_ap_early_settlement_disc as number(38, 10)
            ) as base_ap_early_settlement_disc,

            cast(
                base_ap_growth_incentives as number(38, 10)
            ) as base_ap_growth_incentives,

            cast(base_ap_net_sales_value as number(38, 10)) as base_ap_net_sales_value,

            cast(base_ap_retro as number(38, 10)) as base_ap_retro,

            cast(base_ap_avp_disc as number(38, 10)) as base_ap_avp_disc,

            cast(base_ap_variable_trade as number(38, 10)) as base_ap_variable_trade,

            cast(
                base_ap_promo_fixed_funding as number(38, 10)
            ) as base_ap_promo_fixed_funding,

            cast(
                base_ap_range_support_incentives as number(38, 10)
            ) as base_ap_range_support_incentives,

            cast(
                base_ap_net_net_sales_value as number(38, 10)
            ) as base_ap_net_net_sales_value,

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

            cast(
                base_manso_permanent_disc as number(38, 10)
            ) as base_manso_permanent_disc,

            cast(
                base_manso_off_invoice_disc as number(38, 10)
            ) as base_manso_off_invoice_disc,

            cast(
                base_manso_invoiced_sales_value as number(38, 10)
            ) as base_manso_invoiced_sales_value,

            cast(
                base_manso_early_settlement_disc as number(38, 10)
            ) as base_manso_early_settlement_disc,

            cast(
                base_manso_growth_incentives as number(38, 10)
            ) as base_manso_growth_incentives,

            cast(
                base_manso_net_sales_value as number(38, 10)
            ) as base_manso_net_sales_value,

            cast(base_manso_retro as number(38, 10)) as base_manso_retro,

            cast(base_manso_avp_disc as number(38, 10)) as base_manso_avp_disc,

            cast(
                base_manso_variable_trade as number(38, 10)
            ) as base_manso_variable_trade,

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

            cast(
                base_manso_gcat_standard as number(38, 10)
            ) as base_manso_gcat_standard,

            cast(base_retail_tot_vol_kg as number(38, 10)) as base_retail_tot_vol_kg,

            cast(
                base_ap_retail_revenue_mrrsp as number(38, 10)
            ) as base_ap_retail_revenue_mrrsp,

            cast(
                base_ap_retail_revenue_rsp as number(38, 10)
            ) as base_ap_retail_revenue_rsp,

            cast(
                base_ap_retail_revenue_net as number(38, 10)
            ) as base_ap_retail_revenue_net,

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

            cast(
                base_ap_retail_total_spend as number(38, 10)
            ) as base_ap_retail_total_spend,

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

            cast(
                forecast_ap_permanent_disc as number(38, 10)
            ) as forecast_ap_permanent_disc,

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

            cast(
                forecast_ap_net_sales_value as number(38, 10)
            ) as forecast_ap_net_sales_value,

            cast(forecast_ap_retro as number(38, 10)) as forecast_ap_retro,

            cast(forecast_ap_avp_disc as number(38, 10)) as forecast_ap_avp_disc,

            cast(
                forecast_ap_variable_trade as number(38, 10)
            ) as forecast_ap_variable_trade,

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

            cast(
                forecast_ap_total_trade_gbp as number(38, 10)
            ) as forecast_ap_total_trade_gbp,

            cast(
                forecast_ap_net_realisable_revenue as number(38, 10)
            ) as forecast_ap_net_realisable_revenue,

            cast(
                forecast_ap_tot_prime_cost_standard as number(38, 10)
            ) as forecast_ap_tot_prime_cost_standard,

            cast(
                forecast_ap_gross_margin_standard as number(38, 10)
            ) as forecast_ap_gross_margin_standard,

            cast(
                forecast_ap_gcat_standard as number(38, 10)
            ) as forecast_ap_gcat_standard,

            cast(
                forecast_manso_tot_vol_kg as number(38, 10)
            ) as forecast_manso_tot_vol_kg,

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

            cast(
                forecast_manso_total_trade as number(38, 10)
            ) as forecast_manso_total_trade,

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

            cast(
                forecast_retail_tot_vol_kg as number(38, 10)
            ) as forecast_retail_tot_vol_kg,

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

        from new_table
        qualify row_number() over (partition by unique_key order by 1) = 1
    ),
    final as (
        select *
        from old_fct
        union
        select *
        from new_fct
    )

select *
from final
