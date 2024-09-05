{{
    config(
        tags=["sales", "forecast", "sls_forecast_over"],
    )
}}

with source as (select * from {{ ref("conv_fct_wbx_sls_forecast_override") }})

select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,

    {{ dbt_utils.surrogate_key(["source_system", "source_item_identifier"]) }}
    as item_guid,

    {{
        dbt_utils.surrogate_key(
            [
                "source_system",
                "plan_source_customer_code",
                "'CUSTOMER_MAIN'",
            ]
        )
    }} as plan_customer_addr_number_guid,

    cast(
        substring(plan_source_customer_code, 1, 255) as text(255)
    ) as plan_source_customer_code,

    cast(substring(scen_code, 1, 255) as text(255)) as scen_code,

    {{ dbt_utils.surrogate_key(["source_system", "scen_code"]) }} as scenario_guid,

    cast(calendar_date as timestamp_ntz(9)) as calendar_date,

    cast(snapshot_date as date) as snapshot_date,

    cast(substring(override_flag, 1, 255) as text(255)) as override_flag,

    cast(substring(is_vol_total_nonzero, 1, 20) as text(20)) as is_vol_total_nonzero,

    cast(substring(isonpromo_si, 1, 20) as text(20)) as isonpromo_si,

    cast(substring(isonpromo_so, 1, 20) as text(20)) as isonpromo_so,

    cast(substring(ispreorpostpromo_si, 1, 20) as text(20)) as ispreorpostpromo_si,

    cast(substring(ispreorpostpromo_so, 1, 20) as text(20)) as ispreorpostpromo_so,

    cast(substring(listingactive, 1, 20) as text(20)) as listingactive,

    cast(qty_ca_cannib_loss_si as number(38, 10)) as qty_ca_cannib_loss_si,

    cast(qty_ca_cannib_loss_so as number(38, 10)) as qty_ca_cannib_loss_so,

    cast(qty_ca_effective_base_fc_si as number(38, 10)) as qty_ca_effective_base_fc_si,

    cast(qty_ca_effective_base_fc_so as number(38, 10)) as qty_ca_effective_base_fc_so,

    cast(qty_ca_override_si as number(38, 10)) as qty_ca_override_si,

    cast(qty_ca_override_so as number(38, 10)) as qty_ca_override_so,

    cast(qty_ca_pp_dip_si as number(38, 10)) as qty_ca_pp_dip_si,

    cast(qty_ca_pp_dip_so as number(38, 10)) as qty_ca_pp_dip_so,

    cast(qty_ca_promo_total_si as number(38, 10)) as qty_ca_promo_total_si,

    cast(qty_ca_promo_total_so as number(38, 10)) as qty_ca_promo_total_so,

    cast(qty_ca_si_actual as number(38, 10)) as qty_ca_si_actual,

    cast(qty_ca_so_actual as number(38, 10)) as qty_ca_so_actual,

    cast(qty_ca_stat_base_fc_si as number(38, 10)) as qty_ca_stat_base_fc_si,

    cast(qty_ca_stat_base_fc_so as number(38, 10)) as qty_ca_stat_base_fc_so,

    cast(qty_ca_total_adjust_si as number(38, 10)) as qty_ca_total_adjust_si,

    cast(qty_ca_total_adjust_so as number(38, 10)) as qty_ca_total_adjust_so,

    cast(qty_ca_total_si as number(38, 10)) as qty_ca_total_si,

    cast(qty_ca_total_so as number(38, 10)) as qty_ca_total_so,

    cast(qty_kg_cannib_loss_si as number(38, 10)) as qty_kg_cannib_loss_si,

    cast(qty_kg_cannib_loss_so as number(38, 10)) as qty_kg_cannib_loss_so,

    cast(qty_kg_effective_base_fc_si as number(38, 10)) as qty_kg_effective_base_fc_si,

    cast(qty_kg_effective_base_fc_so as number(38, 10)) as qty_kg_effective_base_fc_so,

    cast(qty_kg_override_si as number(38, 10)) as qty_kg_override_si,

    cast(qty_kg_override_so as number(38, 10)) as qty_kg_override_so,

    cast(qty_kg_pp_dip_si as number(38, 10)) as qty_kg_pp_dip_si,

    cast(qty_kg_pp_dip_so as number(38, 10)) as qty_kg_pp_dip_so,

    cast(qty_kg_promo_total_si as number(38, 10)) as qty_kg_promo_total_si,

    cast(qty_kg_promo_total_so as number(38, 10)) as qty_kg_promo_total_so,

    cast(qty_kg_si_actual as number(38, 10)) as qty_kg_si_actual,

    cast(qty_kg_so_actual as number(38, 10)) as qty_kg_so_actual,

    cast(qty_kg_stat_base_fc_si as number(38, 10)) as qty_kg_stat_base_fc_si,

    cast(qty_kg_stat_base_fc_so as number(38, 10)) as qty_kg_stat_base_fc_so,

    cast(qty_kg_total_adjust_si as number(38, 10)) as qty_kg_total_adjust_si,

    cast(qty_kg_total_adjust_so as number(38, 10)) as qty_kg_total_adjust_so,

    cast(qty_kg_total_si as number(38, 10)) as qty_kg_total_si,

    cast(qty_kg_total_so as number(38, 10)) as qty_kg_total_so,

    cast(qty_ul_cannib_loss_si as number(38, 10)) as qty_ul_cannib_loss_si,

    cast(qty_ul_cannib_loss_so as number(38, 10)) as qty_ul_cannib_loss_so,

    cast(qty_ul_effective_base_fc_si as number(38, 10)) as qty_ul_effective_base_fc_si,

    cast(qty_ul_effective_base_fc_so as number(38, 10)) as qty_ul_effective_base_fc_so,

    cast(qty_ul_override_si as number(38, 10)) as qty_ul_override_si,

    cast(qty_ul_override_so as number(38, 10)) as qty_ul_override_so,

    cast(qty_ul_pp_dip_si as number(38, 10)) as qty_ul_pp_dip_si,

    cast(qty_ul_pp_dip_so as number(38, 10)) as qty_ul_pp_dip_so,

    cast(qty_ul_promo_total_si as number(38, 10)) as qty_ul_promo_total_si,

    cast(qty_ul_promo_total_so as number(38, 10)) as qty_ul_promo_total_so,

    cast(qty_ul_si_actual as number(38, 10)) as qty_ul_si_actual,

    cast(qty_ul_so_actual as number(38, 10)) as qty_ul_so_actual,

    cast(qty_ul_stat_base_fc_si as number(38, 10)) as qty_ul_stat_base_fc_si,

    cast(qty_ul_stat_base_fc_so as number(38, 10)) as qty_ul_stat_base_fc_so,

    cast(qty_ul_total_adjust_si as number(38, 10)) as qty_ul_total_adjust_si,

    cast(qty_ul_total_adjust_so as number(38, 10)) as qty_ul_total_adjust_so,

    cast(qty_ul_total_si as number(38, 10)) as qty_ul_total_si,

    cast(qty_ul_total_so as number(38, 10)) as qty_ul_total_so,

    cast(
        total_baseretentionpercentage as number(38, 10)
    ) as total_baseretentionpercentage,

    cast(
        total_si_preorpostdippercentage as number(38, 10)
    ) as total_si_preorpostdippercentage,

    cast(
        total_so_preorpostdippercentage as number(38, 10)
    ) as total_so_preorpostdippercentage,

    cast(ap_added_value_pack as number(38, 10)) as ap_added_value_pack,

    cast(ap_avp_disc as number(38, 10)) as ap_avp_disc,

    cast(ap_avp_disc_mgmt_adjustment as number(38, 10)) as ap_avp_disc_mgmt_adjustment,

    cast(ap_avp_disc_pre_adjustment as number(38, 10)) as ap_avp_disc_pre_adjustment,

    cast(ap_cash_disc as number(38, 10)) as ap_cash_disc,

    cast(
        ap_cash_disc_mgmt_adjustment as number(38, 10)
    ) as ap_cash_disc_mgmt_adjustment,

    cast(ap_cash_disc_pre_adjustment as number(38, 10)) as ap_cash_disc_pre_adjustment,

    cast(ap_category as number(38, 10)) as ap_category,

    cast(ap_category_mgmt_adjustment as number(38, 10)) as ap_category_mgmt_adjustment,

    cast(ap_category_pre_adjustment as number(38, 10)) as ap_category_pre_adjustment,

    cast(ap_direct_shopper_marketing as number(38, 10)) as ap_direct_shopper_marketing,

    cast(
        ap_direct_shopper_marketing_mgmt_adjustment as number(38, 10)
    ) as ap_direct_shopper_marketing_mgmt_adjustment,

    cast(
        ap_direct_shopper_marketing_pre_adjustment as number(38, 10)
    ) as ap_direct_shopper_marketing_pre_adjustment,

    cast(ap_early_settlement_disc as number(38, 10)) as ap_early_settlement_disc,

    cast(
        ap_early_settlement_disc_mgmt_adjustment as number(38, 10)
    ) as ap_early_settlement_disc_mgmt_adjustment,

    cast(
        ap_early_settlement_disc_pre_adjustment as number(38, 10)
    ) as ap_early_settlement_disc_pre_adjustment,

    cast(ap_everyday_low_prices as number(38, 10)) as ap_everyday_low_prices,

    cast(
        ap_everyday_low_prices_mgmt_adjustment as number(38, 10)
    ) as ap_everyday_low_prices_mgmt_adjustment,

    cast(
        ap_everyday_low_prices_pre_adjustment as number(38, 10)
    ) as ap_everyday_low_prices_pre_adjustment,

    cast(ap_field_marketing as number(38, 10)) as ap_field_marketing,

    cast(
        ap_field_marketing_mgmt_adjustment as number(38, 10)
    ) as ap_field_marketing_mgmt_adjustment,

    cast(
        ap_field_marketing_pre_adjustment as number(38, 10)
    ) as ap_field_marketing_pre_adjustment,

    cast(ap_fixed_annual_payments as number(38, 10)) as ap_fixed_annual_payments,

    cast(
        ap_fixed_annual_payments_mgmt_adjustment as number(38, 10)
    ) as ap_fixed_annual_payments_mgmt_adjustment,

    cast(
        ap_fixed_annual_payments_pre_adjustment as number(38, 10)
    ) as ap_fixed_annual_payments_pre_adjustment,

    cast(
        ap_fixed_trade_cust_invoiced as number(38, 10)
    ) as ap_fixed_trade_cust_invoiced,

    cast(
        ap_fixed_trade_non_cust_invoiced as number(38, 10)
    ) as ap_fixed_trade_non_cust_invoiced,

    cast(ap_gcat_actuals as number(38, 10)) as ap_gcat_actuals,

    cast(ap_gcat_standard as number(38, 10)) as ap_gcat_standard,

    cast(ap_gross_margin_actual as number(38, 10)) as ap_gross_margin_actual,

    cast(ap_gross_margin_standard as number(38, 10)) as ap_gross_margin_standard,

    cast(ap_gross_sales_value as number(38, 10)) as ap_gross_sales_value,

    cast(ap_gross_selling_value as number(38, 10)) as ap_gross_selling_value,

    cast(
        ap_gross_selling_value_mgmt_adjustment as number(38, 10)
    ) as ap_gross_selling_value_mgmt_adjustment,

    cast(
        ap_gross_selling_value_pre_adjustment as number(38, 10)
    ) as ap_gross_selling_value_pre_adjustment,

    cast(ap_growth_incentives as number(38, 10)) as ap_growth_incentives,

    cast(
        ap_growth_incentives_mgmt_adjustment as number(38, 10)
    ) as ap_growth_incentives_mgmt_adjustment,

    cast(
        ap_growth_incentives_pre_adjustment as number(38, 10)
    ) as ap_growth_incentives_pre_adjustment,

    cast(
        ap_indirect_shopper_marketing as number(38, 10)
    ) as ap_indirect_shopper_marketing,

    cast(
        ap_indirect_shopper_marketing_mgmt_adjustment as number(38, 10)
    ) as ap_indirect_shopper_marketing_mgmt_adjustment,

    cast(
        ap_indirect_shopper_marketing_pre_adjustment as number(38, 10)
    ) as ap_indirect_shopper_marketing_pre_adjustment,

    cast(ap_invoiced_sales_value as number(38, 10)) as ap_invoiced_sales_value,

    cast(ap_net_net_sales_value as number(38, 10)) as ap_net_net_sales_value,

    cast(ap_net_realisable_revenue as number(38, 10)) as ap_net_realisable_revenue,

    cast(ap_net_sales_value as number(38, 10)) as ap_net_sales_value,

    cast(ap_off_invoice_disc as number(38, 10)) as ap_off_invoice_disc,

    cast(
        ap_off_invoice_disc_mgmt_adjustment as number(38, 10)
    ) as ap_off_invoice_disc_mgmt_adjustment,

    cast(
        ap_off_invoice_disc_pre_adjustment as number(38, 10)
    ) as ap_off_invoice_disc_pre_adjustment,

    cast(ap_other_direct_payments as number(38, 10)) as ap_other_direct_payments,

    cast(
        ap_other_direct_payments_mgmt_adjustment as number(38, 10)
    ) as ap_other_direct_payments_mgmt_adjustment,

    cast(
        ap_other_direct_payments_pre_adjustment as number(38, 10)
    ) as ap_other_direct_payments_pre_adjustment,

    cast(ap_other_indirect_payments as number(38, 10)) as ap_other_indirect_payments,

    cast(
        ap_other_indirect_payments_mgmt_adjustment as number(38, 10)
    ) as ap_other_indirect_payments_mgmt_adjustment,

    cast(
        ap_other_indirect_payments_pre_adjustment as number(38, 10)
    ) as ap_other_indirect_payments_pre_adjustment,

    cast(ap_permanent_disc as number(38, 10)) as ap_permanent_disc,

    cast(ap_promo_fixed_funding as number(38, 10)) as ap_promo_fixed_funding,

    cast(
        ap_promo_fixed_funding_mgmt_adjustment as number(38, 10)
    ) as ap_promo_fixed_funding_mgmt_adjustment,

    cast(
        ap_promo_fixed_funding_pre_adjustment as number(38, 10)
    ) as ap_promo_fixed_funding_pre_adjustment,

    cast(ap_range_support_allowance as number(38, 10)) as ap_range_support_allowance,

    cast(
        ap_range_support_allowance_mgmt_adjustment as number(38, 10)
    ) as ap_range_support_allowance_mgmt_adjustment,

    cast(
        ap_range_support_allowance_pre_adjustment as number(38, 10)
    ) as ap_range_support_allowance_pre_adjustment,

    cast(ap_range_support_incentives as number(38, 10)) as ap_range_support_incentives,

    cast(
        ap_range_support_incentives_mgmt_adjustment as number(38, 10)
    ) as ap_range_support_incentives_mgmt_adjustment,

    cast(
        ap_range_support_incentives_pre_adjustment as number(38, 10)
    ) as ap_range_support_incentives_pre_adjustment,

    cast(ap_retail_cost_of_sales as number(38, 10)) as ap_retail_cost_of_sales,

    cast(
        ap_retail_margin_excl_fixed_funding as number(38, 10)
    ) as ap_retail_margin_excl_fixed_funding,

    cast(
        ap_retail_margin_incl_fixed_funding as number(38, 10)
    ) as ap_retail_margin_incl_fixed_funding,

    cast(ap_retail_promo_fixed_spend as number(38, 10)) as ap_retail_promo_fixed_spend,

    cast(
        ap_retail_retailer_retro_funding as number(38, 10)
    ) as ap_retail_retailer_retro_funding,

    cast(ap_retail_revenue_mrrsp as number(38, 10)) as ap_retail_revenue_mrrsp,

    cast(ap_retail_revenue_net as number(38, 10)) as ap_retail_revenue_net,

    cast(
        ap_retail_revenue_net_excl_mrrsp as number(38, 10)
    ) as ap_retail_revenue_net_excl_mrrsp,

    cast(
        ap_retail_revenue_net_excl_rsp as number(38, 10)
    ) as ap_retail_revenue_net_excl_rsp,

    cast(ap_retail_revenue_rsp as number(38, 10)) as ap_retail_revenue_rsp,

    cast(ap_retail_total_spend as number(38, 10)) as ap_retail_total_spend,

    cast(ap_retro as number(38, 10)) as ap_retro,

    cast(ap_retro_mgmt_adjustment as number(38, 10)) as ap_retro_mgmt_adjustment,

    cast(ap_retro_pre_adjustment as number(38, 10)) as ap_retro_pre_adjustment,

    cast(ap_tot_prime_cost_standard as number(38, 10)) as ap_tot_prime_cost_standard,

    cast(
        ap_tot_prime_cost_standard_mgmt_adjustment as number(38, 10)
    ) as ap_tot_prime_cost_standard_mgmt_adjustment,

    cast(
        ap_tot_prime_cost_standard_pre_adjustment as number(38, 10)
    ) as ap_tot_prime_cost_standard_pre_adjustment,

    cast(ap_tot_prime_cost_variance as number(38, 10)) as ap_tot_prime_cost_variance,

    cast(
        ap_tot_prime_cost_variance_mgmt_adjustment as number(38, 10)
    ) as ap_tot_prime_cost_variance_mgmt_adjustment,

    cast(
        ap_tot_prime_cost_variance_pre_adjustment as number(38, 10)
    ) as ap_tot_prime_cost_variance_pre_adjustment,

    cast(ap_total_trade as number(38, 10)) as ap_total_trade,

    cast(
        ap_total_trade_cust_invoiced as number(38, 10)
    ) as ap_total_trade_cust_invoiced,

    cast(ap_variable_trade as number(38, 10)) as ap_variable_trade,

    cast(promo_vol as number(38, 10)) as promo_vol,

    cast(promo_vol_kg as number(38, 10)) as promo_vol_kg,

    cast(promo_vol_ul as number(38, 10)) as promo_vol_ul,

    cast(retail_tot_vol_ca as number(38, 10)) as retail_tot_vol_ca,

    cast(retail_tot_vol_kg as number(38, 10)) as retail_tot_vol_kg,

    cast(retail_tot_vol_sgl as number(38, 10)) as retail_tot_vol_sgl,

    cast(retail_tot_vol_sgl_ca as number(38, 10)) as retail_tot_vol_sgl_ca,

    cast(retail_tot_vol_sgl_ul as number(38, 10)) as retail_tot_vol_sgl_ul,

    cast(retail_tot_vol_sp_base_uom as number(38, 10)) as retail_tot_vol_sp_base_uom,

    cast(retail_tot_vol_sp_kg_uom as number(38, 10)) as retail_tot_vol_sp_kg_uom,

    cast(retail_tot_vol_sp_ul_uom as number(38, 10)) as retail_tot_vol_sp_ul_uom,

    cast(retail_tot_vol_ul as number(38, 10)) as retail_tot_vol_ul,

    cast(tot_vol_ca as number(38, 10)) as tot_vol_ca,

    cast(tot_vol_kg as number(38, 10)) as tot_vol_kg,

    cast(tot_vol_sgl as number(38, 10)) as tot_vol_sgl,

    cast(tot_vol_sgl_ca as number(38, 10)) as tot_vol_sgl_ca,

    cast(tot_vol_sgl_ul as number(38, 10)) as tot_vol_sgl_ul,

    cast(tot_vol_sp_base_uom as number(38, 10)) as tot_vol_sp_base_uom,

    cast(
        tot_vol_sp_base_uom_mgmt_adjustment as number(38, 10)
    ) as tot_vol_sp_base_uom_mgmt_adjustment,

    cast(
        tot_vol_sp_base_uom_pre_adjustment as number(38, 10)
    ) as tot_vol_sp_base_uom_pre_adjustment,

    cast(tot_vol_sp_kg_uom as number(38, 10)) as tot_vol_sp_kg_uom,

    cast(
        tot_vol_sp_kg_uom_mgmt_adjustment as number(38, 10)
    ) as tot_vol_sp_kg_uom_mgmt_adjustment,

    cast(
        tot_vol_sp_kg_uom_pre_adjustment as number(38, 10)
    ) as tot_vol_sp_kg_uom_pre_adjustment,

    cast(tot_vol_sp_ul_uom as number(38, 10)) as tot_vol_sp_ul_uom,

    cast(
        tot_vol_sp_ul_uom_mgmt_adjustment as number(38, 10)
    ) as tot_vol_sp_ul_uom_mgmt_adjustment,

    cast(
        tot_vol_sp_ul_uom_pre_adjustment as number(38, 10)
    ) as tot_vol_sp_ul_uom_pre_adjustment,

    cast(tot_vol_ul as number(38, 10)) as tot_vol_ul,

    cast(fcf_tot_vol_kg as number(38, 10)) as fcf_tot_vol_kg,

    cast(fcf_tot_vol_ca as number(38, 10)) as fcf_tot_vol_ca,

    cast(fcf_tot_vol_ul as number(38, 10)) as fcf_tot_vol_ul,

    cast(fcf_base_vol_kg as number(38, 10)) as fcf_base_vol_kg,

    cast(fcf_base_vol_ca as number(38, 10)) as fcf_base_vol_ca,

    cast(fcf_base_vol_ul as number(38, 10)) as fcf_base_vol_ul,

    cast(fcf_promo_vol_kg as number(38, 10)) as fcf_promo_vol_kg,

    cast(fcf_promo_vol_ca as number(38, 10)) as fcf_promo_vol_ca,

    cast(fcf_promo_vol_ul as number(38, 10)) as fcf_promo_vol_ul,

    cast(fcf_over_vol_kg as number(38, 10)) as fcf_over_vol_kg,

    cast(fcf_over_vol_ca as number(38, 10)) as fcf_over_vol_ca,

    cast(fcf_over_vol_ul as number(38, 10)) as fcf_over_vol_ul,

    cast(gl_unit_price as float) as gl_unit_price,

    cast(raw_material_unit_price as float) as raw_material_unit_price,

    cast(ap_tot_prime_cost_standard_raw as float) as ap_tot_prime_cost_standard_raw,

    cast(packaging_unit_price as float) as packaging_unit_price,

    cast(
        ap_tot_prime_cost_standard_packaging as float
    ) as ap_tot_prime_cost_standard_packaging,

    cast(labour_unit_price as float) as labour_unit_price,

    cast(
        ap_tot_prime_cost_standard_labour as float
    ) as ap_tot_prime_cost_standard_labour,

    cast(bought_in_unit_price as float) as bought_in_unit_price,

    cast(
        ap_tot_prime_cost_standard_bought_in as float
    ) as ap_tot_prime_cost_standard_bought_in,

    cast(other_unit_price as float) as other_unit_price,

    cast(ap_tot_prime_cost_standard_other as float) as ap_tot_prime_cost_standard_other,

    cast(co_pack_unit_price as float) as co_pack_unit_price,

    cast(
        ap_tot_prime_cost_standard_co_pack as float
    ) as ap_tot_prime_cost_standard_co_pack,

    cast(rate_ap_added_value_pack as float) as rate_ap_added_value_pack,

    cast(rate_ap_permanent_disc as float) as rate_ap_permanent_disc,

    cast(rate_ap_invoiced_sales_value as float) as rate_ap_invoiced_sales_value,

    cast(rate_ap_net_sales_value as float) as rate_ap_net_sales_value,

    cast(rate_ap_net_realisable_revenue as float) as rate_ap_net_realisable_revenue,

    cast(rate_ap_variable_trade as float) as rate_ap_variable_trade,

    cast(rate_ap_net_net_sales_value as float) as rate_ap_net_net_sales_value,

    cast(
        rate_ap_fixed_trade_cust_invoiced as float
    ) as rate_ap_fixed_trade_cust_invoiced,

    cast(
        rate_ap_total_trade_cust_invoiced as float
    ) as rate_ap_total_trade_cust_invoiced,

    cast(
        rate_ap_fixed_trade_non_cust_invoiced as float
    ) as rate_ap_fixed_trade_non_cust_invoiced,

    cast(rate_ap_total_trade as float) as rate_ap_total_trade,

    cast(rate_ap_gross_margin_standard as float) as rate_ap_gross_margin_standard,

    cast(rate_ap_gross_margin_actual as float) as rate_ap_gross_margin_actual,

    cast(rate_ap_gcat_standard as float) as rate_ap_gcat_standard,

    cast(rate_ap_gcat_actuals as float) as rate_ap_gcat_actuals,

    cast(
        rate_ap_fixed_annual_payments_pre_adj as float
    ) as rate_ap_fixed_annual_payments_pre_adj,

    cast(
        rate_ap_fixed_annual_payments_mgmt_adj as float
    ) as rate_ap_fixed_annual_payments_mgmt_adj,

    cast(rate_ap_fixed_annual_payments as float) as rate_ap_fixed_annual_payments,

    cast(rate_ap_category_pre_adj as float) as rate_ap_category_pre_adj,

    cast(rate_ap_category_mgmt_adj as float) as rate_ap_category_mgmt_adj,

    cast(rate_ap_category as float) as rate_ap_category,

    cast(
        rate_ap_promo_fixed_funding_pre_adj as float
    ) as rate_ap_promo_fixed_funding_pre_adj,

    cast(
        rate_ap_promo_fixed_funding_mgmt_adj as float
    ) as rate_ap_promo_fixed_funding_mgmt_adj,

    cast(rate_ap_promo_fixed_funding as float) as rate_ap_promo_fixed_funding,

    cast(rate_ap_cash_disc_pre_adj as float) as rate_ap_cash_disc_pre_adj,

    cast(rate_ap_cash_disc_mgmt_adj as float) as rate_ap_cash_disc_mgmt_adj,

    cast(rate_ap_cash_disc as float) as rate_ap_cash_disc,

    cast(
        rate_ap_direct_shopper_marketing_pre_adj as float
    ) as rate_ap_direct_shopper_marketing_pre_adj,

    cast(
        rate_ap_direct_shopper_marketing_mgmt_adj as float
    ) as rate_ap_direct_shopper_marketing_mgmt_adj,

    cast(rate_ap_direct_shopper_marketing as float) as rate_ap_direct_shopper_marketing,

    cast(
        rate_ap_range_support_allowance_pre_adj as float
    ) as rate_ap_range_support_allowance_pre_adj,

    cast(
        rate_ap_range_support_allowance_mgmt_adj as float
    ) as rate_ap_range_support_allowance_mgmt_adj,

    cast(rate_ap_range_support_allowance as float) as rate_ap_range_support_allowance,

    cast(
        rate_ap_range_support_incentives_pre_adj as float
    ) as rate_ap_range_support_incentives_pre_adj,

    cast(
        rate_ap_range_support_incentives_mgmt_adj as float
    ) as rate_ap_range_support_incentives_mgmt_adj,

    cast(rate_ap_range_support_incentives as float) as rate_ap_range_support_incentives,

    cast(
        rate_ap_indirect_shopper_marketing_pre_adj as float
    ) as rate_ap_indirect_shopper_marketing_pre_adj,

    cast(
        rate_ap_indirect_shopper_marketing_mgmt_adj as float
    ) as rate_ap_indirect_shopper_marketing_mgmt_adj,

    cast(
        rate_ap_indirect_shopper_marketing as float
    ) as rate_ap_indirect_shopper_marketing,

    cast(rate_ap_retro_pre_adj as float) as rate_ap_retro_pre_adj,

    cast(rate_ap_retro_mgmt_adj as float) as rate_ap_retro_mgmt_adj,

    cast(rate_ap_retro as float) as rate_ap_retro,

    cast(rate_ap_avp_disc_pre_adj as float) as rate_ap_avp_disc_pre_adj,

    cast(rate_ap_avp_disc_mgmt_adj as float) as rate_ap_avp_disc_mgmt_adj,

    cast(rate_ap_avp_disc as float) as rate_ap_avp_disc,

    cast(
        rate_ap_everyday_low_prices_pre_adj as float
    ) as rate_ap_everyday_low_prices_pre_adj,

    cast(
        rate_ap_everyday_low_prices_mgmt_adj as float
    ) as rate_ap_everyday_low_prices_mgmt_adj,

    cast(rate_ap_everyday_low_prices as float) as rate_ap_everyday_low_prices,

    cast(rate_ap_off_invoice_disc_pre_adj as float) as rate_ap_off_invoice_disc_pre_adj,

    cast(
        rate_ap_off_invoice_disc_mgmt_adj as float
    ) as rate_ap_off_invoice_disc_mgmt_adj,

    cast(rate_ap_off_invoice_disc as float) as rate_ap_off_invoice_disc,

    cast(rate_ap_field_marketing_pre_adj as float) as rate_ap_field_marketing_pre_adj,

    cast(rate_ap_field_marketing_mgmt_adj as float) as rate_ap_field_marketing_mgmt_adj,

    cast(rate_ap_field_marketing as float) as rate_ap_field_marketing,

    cast(
        rate_ap_tot_prime_cost_variance_pre_adj as float
    ) as rate_ap_tot_prime_cost_variance_pre_adj,

    cast(
        rate_ap_tot_prime_cost_variance_mgmt_adj as float
    ) as rate_ap_tot_prime_cost_variance_mgmt_adj,

    cast(rate_ap_tot_prime_cost_variance as float) as rate_ap_tot_prime_cost_variance,

    cast(
        rate_ap_tot_prime_cost_standard_pre_adj as float
    ) as rate_ap_tot_prime_cost_standard_pre_adj,

    cast(
        rate_ap_tot_prime_cost_standard_mgmt_adj as float
    ) as rate_ap_tot_prime_cost_standard_mgmt_adj,

    cast(rate_ap_tot_prime_cost_standard as float) as rate_ap_tot_prime_cost_standard,

    cast(
        rate_ap_early_settlement_disc_pre_adj as float
    ) as rate_ap_early_settlement_disc_pre_adj,

    cast(
        rate_ap_early_settlement_disc_mgmt_adj as float
    ) as rate_ap_early_settlement_disc_mgmt_adj,

    cast(rate_ap_early_settlement_disc as float) as rate_ap_early_settlement_disc,

    cast(
        rate_ap_other_direct_payments_pre_adj as float
    ) as rate_ap_other_direct_payments_pre_adj,

    cast(
        rate_ap_other_direct_payments_mgmt_adj as float
    ) as rate_ap_other_direct_payments_mgmt_adj,

    cast(rate_ap_other_direct_payments as float) as rate_ap_other_direct_payments,

    cast(
        rate_ap_other_indirect_payments_pre_adj as float
    ) as rate_ap_other_indirect_payments_pre_adj,

    cast(
        rate_ap_other_indirect_payments_mgmt_adj as float
    ) as rate_ap_other_indirect_payments_mgmt_adj,

    cast(rate_ap_other_indirect_payments as float) as rate_ap_other_indirect_payments,

    cast(
        rate_ap_gross_selling_value_pre_adj as float
    ) as rate_ap_gross_selling_value_pre_adj,

    cast(
        rate_ap_gross_selling_value_mgmt_adj as float
    ) as rate_ap_gross_selling_value_mgmt_adj,

    cast(rate_ap_gross_selling_value as float) as rate_ap_gross_selling_value,

    cast(rate_ap_gross_sales_value as float) as rate_ap_gross_sales_value,

    cast(
        rate_ap_growth_incentives_pre_adj as float
    ) as rate_ap_growth_incentives_pre_adj,

    cast(
        rate_ap_growth_incentives_mgmt_adj as float
    ) as rate_ap_growth_incentives_mgmt_adj,

    cast(rate_ap_growth_incentives as float) as rate_ap_growth_incentives,

    cast(rate_retail_tot_vol_sgl as float) as rate_retail_tot_vol_sgl,

    cast(rate_retail_tot_vol_sgl_ca as float) as rate_retail_tot_vol_sgl_ca,

    cast(rate_retail_tot_vol_sgl_ul as float) as rate_retail_tot_vol_sgl_ul,

    cast(rate_retail_tot_vol_kg as float) as rate_retail_tot_vol_kg,

    cast(rate_retail_tot_vol_ca as float) as rate_retail_tot_vol_ca,

    cast(rate_retail_tot_vol_ul as float) as rate_retail_tot_vol_ul,

    cast(rate_ap_retail_revenue_mrrsp as float) as rate_ap_retail_revenue_mrrsp,

    cast(rate_ap_retail_revenue_rsp as float) as rate_ap_retail_revenue_rsp,

    cast(rate_ap_retail_revenue_net as float) as rate_ap_retail_revenue_net,

    cast(rate_ap_retail_cost_of_sales as float) as rate_ap_retail_cost_of_sales,

    cast(
        rate_ap_retail_retailer_retro_funding as float
    ) as rate_ap_retail_retailer_retro_funding,

    cast(
        rate_ap_retail_margin_excl_fixed_funding as float
    ) as rate_ap_retail_margin_excl_fixed_funding,

    cast(rate_ap_retail_promo_fixed_spend as float) as rate_ap_retail_promo_fixed_spend,

    cast(rate_ap_retail_total_spend as float) as rate_ap_retail_total_spend,

    cast(
        rate_ap_retail_margin_incl_fixed_funding as float
    ) as rate_ap_retail_margin_incl_fixed_funding,

    cast(
        rate_ap_retail_revenue_net_excl_mrrsp as float
    ) as rate_ap_retail_revenue_net_excl_mrrsp,

    cast(
        rate_ap_retail_revenue_net_excl_rsp as float
    ) as rate_ap_retail_revenue_net_excl_rsp,

    cast(fcf_tot_orig_vol_kg as number(38, 10)) as fcf_tot_orig_vol_kg,

    cast(fcf_tot_orig_vol_ca as number(38, 10)) as fcf_tot_orig_vol_ca,

    cast(fcf_tot_orig_vol_ul as number(38, 10)) as fcf_tot_orig_vol_ul
    
from source
