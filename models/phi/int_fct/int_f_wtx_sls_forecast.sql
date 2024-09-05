   {{
    config(
        tags = ["sls","sales","forecast","sls_forecast","sls_finance"],
        snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    )
}}
with sls_forecast as (
    select * from {{ ref('fct_wbx_sls_forecast_sls')}}
    where date_trunc(day,snapshot_date) =  (select snap.snapshot_date from {{ ref('stg_d_wtx_lkp_snapshot_date')}} snap)
),
fin_forecast as (
    select * from {{ ref('fct_wbx_sls_forecast_fin')}}
    where date_trunc(day,snapshot_date) =  (select snap.snapshot_date from {{ ref('stg_d_wtx_lkp_snapshot_date')}} snap)
),
final as (
    select 
        nvl(a.source_system,b.source_system) as source_system,
        nvl(a.source_item_identifier,b.source_item_identifier) as source_item_identifier,
        nvl(a.item_guid,b.item_guid) as item_guid,
        nvl(a.plan_source_customer_code,b.plan_source_customer_code) as plan_source_customer_code,
        nvl(a.plan_customer_addr_number_guid,b.customer_address_number_guid) as plan_customer_addr_number_guid,
        nvl(a.scen_code,b.scen_code) as scen_code,
        nvl(a.scenario_guid,b.scenario_guid) as scenario_guid,
        nvl(a.calendar_date,b.calendar_date) as calendar_date,
        nvl(a.snapshot_date,b.snapshot_date) as snapshot_date,
        a.isonpromo_si,
        a.isonpromo_so,
        a.ispreorpostpromo_si,
        a.ispreorpostpromo_so,
        a.listingactive,
        a.total_baseretentionpercentage,
        a.total_si_preorpostdippercentage,
        a.total_so_preorpostdippercentage,
        a.is_vol_total_nonzero,
        a.qty_ca_stat_base_fc_si,
        a.qty_ca_stat_base_fc_so,
        a.qty_ca_override_si,
        a.qty_ca_override_so,
        a.qty_ca_effective_base_fc_si,
        a.qty_ca_effective_base_fc_so,
        a.qty_ca_promo_total_si,
        a.qty_ca_promo_total_so,
        a.qty_ca_cannib_loss_si,
        a.qty_ca_cannib_loss_so,
        a.qty_ca_pp_dip_si,
        a.qty_ca_pp_dip_so,
        a.qty_ca_total_si,
        a.qty_ca_total_so,
        a.qty_ca_si_actual,
        a.qty_ca_so_actual,
        a.qty_ca_total_adjust_si,
        a.qty_ca_total_adjust_so,
        a.qty_kg_stat_base_fc_si,
        a.qty_kg_stat_base_fc_so,
        a.qty_kg_override_si,
        a.qty_kg_override_so,
        a.qty_kg_effective_base_fc_si,
        a.qty_kg_effective_base_fc_so,
        a.qty_kg_promo_total_si,
        a.qty_kg_promo_total_so,
        a.qty_kg_cannib_loss_si,
        a.qty_kg_cannib_loss_so,
        a.qty_kg_pp_dip_si,
        a.qty_kg_pp_dip_so,
        a.qty_kg_total_si,
        a.qty_kg_total_so,
        a.qty_kg_si_actual,
        a.qty_kg_so_actual,
        a.qty_kg_total_adjust_si,
        a.qty_kg_total_adjust_so,
        a.qty_ul_stat_base_fc_si,
        a.qty_ul_stat_base_fc_so,
        a.qty_ul_override_si,
        a.qty_ul_override_so,
        a.qty_ul_effective_base_fc_si,
        a.qty_ul_effective_base_fc_so,
        a.qty_ul_promo_total_si,
        a.qty_ul_promo_total_so,
        a.qty_ul_cannib_loss_si,
        a.qty_ul_cannib_loss_so,
        a.qty_ul_pp_dip_si,
        a.qty_ul_pp_dip_so,
        a.qty_ul_total_si,
        a.qty_ul_total_so,
        a.qty_ul_si_actual,
        a.qty_ul_so_actual,
        a.qty_ul_total_adjust_si,
        a.qty_ul_total_adjust_so,

        b.ap_added_value_pack,
        b.ap_avp_disc,
        b.ap_avp_disc_mgmt_adjustment,
        b.ap_avp_disc_pre_adjustment,
        b.ap_cash_disc,
        b.ap_cash_disc_mgmt_adjustment,
        b.ap_cash_disc_pre_adjustment,
        b.ap_category,
        b.ap_category_mgmt_adjustment,
        b.ap_category_pre_adjustment,
        b.ap_direct_shopper_marketing,
        b.ap_direct_shopper_marketing_mgmt_adjustment,
        b.ap_direct_shopper_marketing_pre_adjustment,
        b.ap_early_settlement_disc,
        b.ap_early_settlement_disc_mgmt_adjustment,
        b.ap_early_settlement_disc_pre_adjustment,
        b.ap_everyday_low_prices,
        b.ap_everyday_low_prices_mgmt_adjustment,
        b.ap_everyday_low_prices_pre_adjustment,
        b.ap_field_marketing,
        b.ap_field_marketing_mgmt_adjustment,
        b.ap_field_marketing_pre_adjustment,
        b.ap_fixed_annual_payments,
        b.ap_fixed_annual_payments_mgmt_adjustment,
        b.ap_fixed_annual_payments_pre_adjustment,
        b.ap_fixed_trade_cust_invoiced,
        b.ap_fixed_trade_non_cust_invoiced,
        b.ap_gcat_actuals,
        b.ap_gcat_standard,
        b.ap_gross_margin_actual,
        b.ap_gross_margin_standard,
        b.ap_gross_sales_value,
        b.ap_gross_selling_value,
        b.ap_gross_selling_value_mgmt_adjustment,
        b.ap_gross_selling_value_pre_adjustment,
        b.ap_growth_incentives,
        b.ap_growth_incentives_mgmt_adjustment,
        b.ap_growth_incentives_pre_adjustment,
        b.ap_indirect_shopper_marketing,
        b.ap_indirect_shopper_marketing_mgmt_adjustment,
        b.ap_indirect_shopper_marketing_pre_adjustment,
        b.ap_invoiced_sales_value,
        b.ap_net_net_sales_value,
        b.ap_net_realisable_revenue,
        b.ap_net_sales_value,
        b.ap_off_invoice_disc,
        b.ap_off_invoice_disc_mgmt_adjustment,
        b.ap_off_invoice_disc_pre_adjustment,
        b.ap_other_direct_payments,
        b.ap_other_direct_payments_mgmt_adjustment,
        b.ap_other_direct_payments_pre_adjustment,
        b.ap_other_indirect_payments,
        b.ap_other_indirect_payments_mgmt_adjustment,
        b.ap_other_indirect_payments_pre_adjustment,
        b.ap_permanent_disc,
        b.ap_promo_fixed_funding,
        b.ap_promo_fixed_funding_mgmt_adjustment,
        b.ap_promo_fixed_funding_pre_adjustment,
        b.ap_range_support_allowance,
        b.ap_range_support_allowance_mgmt_adjustment,
        b.ap_range_support_allowance_pre_adjustment,
        b.ap_range_support_incentives,
        b.ap_range_support_incentives_mgmt_adjustment,
        b.ap_range_support_incentives_pre_adjustment,
        b.ap_retail_cost_of_sales,
        b.ap_retail_margin_excl_fixed_funding,
        b.ap_retail_margin_incl_fixed_funding,
        b.ap_retail_promo_fixed_spend,
        b.ap_retail_retailer_retro_funding,
        b.ap_retail_revenue_mrrsp,
        b.ap_retail_revenue_net,
        b.ap_retail_revenue_net_excl_mrrsp,
        b.ap_retail_revenue_net_excl_rsp,
        b.ap_retail_revenue_rsp,
        b.ap_retail_total_spend,
        b.ap_retro,
        b.ap_retro_mgmt_adjustment,
        b.ap_retro_pre_adjustment,
        b.ap_tot_prime_cost_standard,
        b.ap_tot_prime_cost_standard_mgmt_adjustment,
        b.ap_tot_prime_cost_standard_pre_adjustment,
        b.ap_tot_prime_cost_variance,
        b.ap_tot_prime_cost_variance_mgmt_adjustment,
        b.ap_tot_prime_cost_variance_pre_adjustment,
        b.ap_total_trade,
        b.ap_total_trade_cust_invoiced,
        b.ap_variable_trade,
        b.promo_vol,
        b.promo_vol_kg,
        b.promo_vol_ul,
        nvl(b.retail_tot_vol_ca,0) as retail_tot_vol_ca,
        b.retail_tot_vol_kg,
        b.retail_tot_vol_sgl,
        b.retail_tot_vol_sgl_ca,
        b.retail_tot_vol_sgl_ul,
        b.retail_tot_vol_sp_base_uom,
        b.retail_tot_vol_sp_kg_uom,
        b.retail_tot_vol_sp_ul_uom,
        nvl(b.retail_tot_vol_ul,0) as retail_tot_vol_ul,
        b.tot_vol_ca,
        b.tot_vol_kg,
        b.tot_vol_sgl,
        b.tot_vol_sgl_ca,
        b.tot_vol_sgl_ul,
        b.tot_vol_sp_base_uom,
        b.tot_vol_sp_base_uom_mgmt_adjustment,
        b.tot_vol_sp_base_uom_pre_adjustment,
        b.tot_vol_sp_kg_uom,
        b.tot_vol_sp_kg_uom_mgmt_adjustment,
        b.tot_vol_sp_kg_uom_pre_adjustment,
        b.tot_vol_sp_ul_uom,
        b.tot_vol_sp_ul_uom_mgmt_adjustment,
        b.tot_vol_sp_ul_uom_pre_adjustment,
        b.tot_vol_ul,
        b.gl_unit_price,
        nvl(b.raw_material_unit_price,0) as raw_material_unit_price,
        nvl(b.ap_tot_prime_cost_standard_raw,0) as ap_tot_prime_cost_standard_raw,
        nvl(b.packaging_unit_price,0) as packaging_unit_price,
        nvl(b.ap_tot_prime_cost_standard_packaging,0) as ap_tot_prime_cost_standard_packaging,
        nvl(b.labour_unit_price,0) as labour_unit_price,
        nvl(b.ap_tot_prime_cost_standard_labour,0) as ap_tot_prime_cost_standard_labour,
        nvl(b.bought_in_unit_price,0) as bought_in_unit_price,
        nvl(b.ap_tot_prime_cost_standard_bought_in,0) as ap_tot_prime_cost_standard_bought_in,
        nvl(b.other_unit_price,0) as other_unit_price,
        nvl(b.ap_tot_prime_cost_standard_other,0) as ap_tot_prime_cost_standard_other,
        nvl(b.co_pack_unit_price,0) as co_pack_unit_price,
        nvl(b.ap_tot_prime_cost_standard_co_pack,0) as ap_tot_prime_cost_standard_co_pack,

        b.tot_vol_kg as fcf_tot_vol_kg,
        b.tot_vol_ca as fcf_tot_vol_ca,
        b.tot_vol_ul as fcf_tot_vol_ul,
        a.qty_kg_effective_base_fc_si as fcf_base_vol_kg,
        a.qty_ca_effective_base_fc_si as fcf_base_vol_ca,
        a.qty_ul_effective_base_fc_si as fcf_base_vol_ul,
        (b.tot_vol_kg - a.qty_kg_effective_base_fc_si) as fcf_promo_vol_kg,
        (b.tot_vol_ca - a.qty_ca_effective_base_fc_si) as fcf_promo_vol_ca,
        (b.tot_vol_ul - a.qty_ul_effective_base_fc_si) as fcf_promo_vol_ul,
        0 as fcf_over_vol_kg,
        0 as fcf_over_vol_ca ,
        0 as fcf_over_vol_ul
    from sls_forecast a
    full outer join fin_forecast b
    on a.source_system = b.source_system
    and trim(a.source_item_identifier) = trim(b.source_item_identifier)
    and trim(a.plan_source_customer_code) = trim(b.plan_source_customer_code)
    and a.scen_code = b.scen_code
    and date_trunc(day,a.calendar_date) = date_trunc(day,b.calendar_date)
    and date_trunc(day,a.snapshot_date) = date_trunc(day,b.snapshot_date)
)

select 
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,
    cast(substring(plan_source_customer_code,1,255) as text(255) ) as plan_source_customer_code  ,
    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
    cast(calendar_date as timestamp_ntz(9) ) as calendar_date  ,
    cast(snapshot_date as date) as snapshot_date  ,
   
    cast(substring(is_vol_total_nonzero,1,20) as text(20) ) as is_vol_total_nonzero  ,
    cast(substring(isonpromo_si,1,20) as text(20) ) as isonpromo_si  ,

    cast(substring(isonpromo_so,1,20) as text(20) ) as isonpromo_so  ,

    cast(substring(ispreorpostpromo_si,1,20) as text(20) ) as ispreorpostpromo_si  ,

    cast(substring(ispreorpostpromo_so,1,20) as text(20) ) as ispreorpostpromo_so  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(substring(listingactive,1,20) as text(20) ) as listingactive  ,

    cast(plan_customer_addr_number_guid as text(255) ) as plan_customer_addr_number_guid  ,

    cast(qty_ca_cannib_loss_si as number(38,10) ) as qty_ca_cannib_loss_si  ,

    cast(qty_ca_cannib_loss_so as number(38,10) ) as qty_ca_cannib_loss_so  ,

    cast(qty_ca_effective_base_fc_si as number(38,10) ) as qty_ca_effective_base_fc_si  ,

    cast(qty_ca_effective_base_fc_so as number(38,10) ) as qty_ca_effective_base_fc_so  ,

    cast(qty_ca_override_si as number(38,10) ) as qty_ca_override_si  ,

    cast(qty_ca_override_so as number(38,10) ) as qty_ca_override_so  ,

    cast(qty_ca_pp_dip_si as number(38,10) ) as qty_ca_pp_dip_si  ,

    cast(qty_ca_pp_dip_so as number(38,10) ) as qty_ca_pp_dip_so  ,

    cast(qty_ca_promo_total_si as number(38,10) ) as qty_ca_promo_total_si  ,

    cast(qty_ca_promo_total_so as number(38,10) ) as qty_ca_promo_total_so  ,

    cast(qty_ca_si_actual as number(38,10) ) as qty_ca_si_actual  ,

    cast(qty_ca_so_actual as number(38,10) ) as qty_ca_so_actual  ,

    cast(qty_ca_stat_base_fc_si as number(38,10) ) as qty_ca_stat_base_fc_si  ,

    cast(qty_ca_stat_base_fc_so as number(38,10) ) as qty_ca_stat_base_fc_so  ,

    cast(qty_ca_total_adjust_si as number(38,10) ) as qty_ca_total_adjust_si  ,

    cast(qty_ca_total_adjust_so as number(38,10) ) as qty_ca_total_adjust_so  ,

    cast(qty_ca_total_si as number(38,10) ) as qty_ca_total_si  ,

    cast(qty_ca_total_so as number(38,10) ) as qty_ca_total_so  ,

    cast(qty_kg_cannib_loss_si as number(38,10) ) as qty_kg_cannib_loss_si  ,

    cast(qty_kg_cannib_loss_so as number(38,10) ) as qty_kg_cannib_loss_so  ,

    cast(qty_kg_effective_base_fc_si as number(38,10) ) as qty_kg_effective_base_fc_si  ,

    cast(qty_kg_effective_base_fc_so as number(38,10) ) as qty_kg_effective_base_fc_so  ,

    cast(qty_kg_override_si as number(38,10) ) as qty_kg_override_si  ,

    cast(qty_kg_override_so as number(38,10) ) as qty_kg_override_so  ,

    cast(qty_kg_pp_dip_si as number(38,10) ) as qty_kg_pp_dip_si  ,

    cast(qty_kg_pp_dip_so as number(38,10) ) as qty_kg_pp_dip_so  ,

    cast(qty_kg_promo_total_si as number(38,10) ) as qty_kg_promo_total_si  ,

    cast(qty_kg_promo_total_so as number(38,10) ) as qty_kg_promo_total_so  ,

    cast(qty_kg_si_actual as number(38,10) ) as qty_kg_si_actual  ,

    cast(qty_kg_so_actual as number(38,10) ) as qty_kg_so_actual  ,

    cast(qty_kg_stat_base_fc_si as number(38,10) ) as qty_kg_stat_base_fc_si  ,

    cast(qty_kg_stat_base_fc_so as number(38,10) ) as qty_kg_stat_base_fc_so  ,

    cast(qty_kg_total_adjust_si as number(38,10) ) as qty_kg_total_adjust_si  ,

    cast(qty_kg_total_adjust_so as number(38,10) ) as qty_kg_total_adjust_so  ,

    cast(qty_kg_total_si as number(38,10) ) as qty_kg_total_si  ,

    cast(qty_kg_total_so as number(38,10) ) as qty_kg_total_so  ,

    cast(qty_ul_cannib_loss_si as number(38,10) ) as qty_ul_cannib_loss_si  ,

    cast(qty_ul_cannib_loss_so as number(38,10) ) as qty_ul_cannib_loss_so  ,

    cast(qty_ul_effective_base_fc_si as number(38,10) ) as qty_ul_effective_base_fc_si  ,

    cast(qty_ul_effective_base_fc_so as number(38,10) ) as qty_ul_effective_base_fc_so  ,

    cast(qty_ul_override_si as number(38,10) ) as qty_ul_override_si  ,

    cast(qty_ul_override_so as number(38,10) ) as qty_ul_override_so  ,

    cast(qty_ul_pp_dip_si as number(38,10) ) as qty_ul_pp_dip_si  ,

    cast(qty_ul_pp_dip_so as number(38,10) ) as qty_ul_pp_dip_so  ,

    cast(qty_ul_promo_total_si as number(38,10) ) as qty_ul_promo_total_si  ,

    cast(qty_ul_promo_total_so as number(38,10) ) as qty_ul_promo_total_so  ,

    cast(qty_ul_si_actual as number(38,10) ) as qty_ul_si_actual  ,

    cast(qty_ul_so_actual as number(38,10) ) as qty_ul_so_actual  ,

    cast(qty_ul_stat_base_fc_si as number(38,10) ) as qty_ul_stat_base_fc_si  ,

    cast(qty_ul_stat_base_fc_so as number(38,10) ) as qty_ul_stat_base_fc_so  ,

    cast(qty_ul_total_adjust_si as number(38,10) ) as qty_ul_total_adjust_si  ,

    cast(qty_ul_total_adjust_so as number(38,10) ) as qty_ul_total_adjust_so  ,

    cast(qty_ul_total_si as number(38,10) ) as qty_ul_total_si  ,

    cast(qty_ul_total_so as number(38,10) ) as qty_ul_total_so  ,

    cast(substring(scen_code,1,255) as text(255) ) as scen_code  ,

    cast(scenario_guid as text(255) ) as scenario_guid  ,

    cast(total_baseretentionpercentage as number(38,10) ) as total_baseretentionpercentage  ,

    cast(total_si_preorpostdippercentage as number(38,10) ) as total_si_preorpostdippercentage  ,

    cast(total_so_preorpostdippercentage as number(38,10) ) as total_so_preorpostdippercentage  ,

    cast(ap_added_value_pack as number(38,10) ) as ap_added_value_pack  ,

    cast(ap_avp_disc as number(38,10) ) as ap_avp_disc  ,

    cast(ap_avp_disc_mgmt_adjustment as number(38,10) ) as ap_avp_disc_mgmt_adjustment  ,

    cast(ap_avp_disc_pre_adjustment as number(38,10) ) as ap_avp_disc_pre_adjustment  ,

    cast(ap_cash_disc as number(38,10) ) as ap_cash_disc  ,

    cast(ap_cash_disc_mgmt_adjustment as number(38,10) ) as ap_cash_disc_mgmt_adjustment  ,

    cast(ap_cash_disc_pre_adjustment as number(38,10) ) as ap_cash_disc_pre_adjustment  ,

    cast(ap_category as number(38,10) ) as ap_category  ,

    cast(ap_category_mgmt_adjustment as number(38,10) ) as ap_category_mgmt_adjustment  ,

    cast(ap_category_pre_adjustment as number(38,10) ) as ap_category_pre_adjustment  ,

    cast(ap_direct_shopper_marketing as number(38,10) ) as ap_direct_shopper_marketing  ,

    cast(ap_direct_shopper_marketing_mgmt_adjustment as number(38,10) ) as ap_direct_shopper_marketing_mgmt_adjustment  ,

    cast(ap_direct_shopper_marketing_pre_adjustment as number(38,10) ) as ap_direct_shopper_marketing_pre_adjustment  ,

    cast(ap_early_settlement_disc as number(38,10) ) as ap_early_settlement_disc  ,

    cast(ap_early_settlement_disc_mgmt_adjustment as number(38,10) ) as ap_early_settlement_disc_mgmt_adjustment  ,

    cast(ap_early_settlement_disc_pre_adjustment as number(38,10) ) as ap_early_settlement_disc_pre_adjustment  ,

    cast(ap_everyday_low_prices as number(38,10) ) as ap_everyday_low_prices  ,

    cast(ap_everyday_low_prices_mgmt_adjustment as number(38,10) ) as ap_everyday_low_prices_mgmt_adjustment  ,

    cast(ap_everyday_low_prices_pre_adjustment as number(38,10) ) as ap_everyday_low_prices_pre_adjustment  ,

    cast(ap_field_marketing as number(38,10) ) as ap_field_marketing  ,

    cast(ap_field_marketing_mgmt_adjustment as number(38,10) ) as ap_field_marketing_mgmt_adjustment  ,

    cast(ap_field_marketing_pre_adjustment as number(38,10) ) as ap_field_marketing_pre_adjustment  ,

    cast(ap_fixed_annual_payments as number(38,10) ) as ap_fixed_annual_payments  ,

    cast(ap_fixed_annual_payments_mgmt_adjustment as number(38,10) ) as ap_fixed_annual_payments_mgmt_adjustment  ,

    cast(ap_fixed_annual_payments_pre_adjustment as number(38,10) ) as ap_fixed_annual_payments_pre_adjustment  ,

    cast(ap_fixed_trade_cust_invoiced as number(38,10) ) as ap_fixed_trade_cust_invoiced  ,

    cast(ap_fixed_trade_non_cust_invoiced as number(38,10) ) as ap_fixed_trade_non_cust_invoiced  ,

    cast(ap_gcat_actuals as number(38,10) ) as ap_gcat_actuals  ,

    cast(ap_gcat_standard as number(38,10) ) as ap_gcat_standard  ,

    cast(ap_gross_margin_actual as number(38,10) ) as ap_gross_margin_actual  ,

    cast(ap_gross_margin_standard as number(38,10) ) as ap_gross_margin_standard  ,

    cast(ap_gross_sales_value as number(38,10) ) as ap_gross_sales_value  ,

    cast(ap_gross_selling_value as number(38,10) ) as ap_gross_selling_value  ,

    cast(ap_gross_selling_value_mgmt_adjustment as number(38,10) ) as ap_gross_selling_value_mgmt_adjustment  ,

    cast(ap_gross_selling_value_pre_adjustment as number(38,10) ) as ap_gross_selling_value_pre_adjustment  ,

    cast(ap_growth_incentives as number(38,10) ) as ap_growth_incentives  ,

    cast(ap_growth_incentives_mgmt_adjustment as number(38,10) ) as ap_growth_incentives_mgmt_adjustment  ,

    cast(ap_growth_incentives_pre_adjustment as number(38,10) ) as ap_growth_incentives_pre_adjustment  ,

    cast(ap_indirect_shopper_marketing as number(38,10) ) as ap_indirect_shopper_marketing  ,

    cast(ap_indirect_shopper_marketing_mgmt_adjustment as number(38,10) ) as ap_indirect_shopper_marketing_mgmt_adjustment  ,

    cast(ap_indirect_shopper_marketing_pre_adjustment as number(38,10) ) as ap_indirect_shopper_marketing_pre_adjustment  ,

    cast(ap_invoiced_sales_value as number(38,10) ) as ap_invoiced_sales_value  ,

    cast(ap_net_net_sales_value as number(38,10) ) as ap_net_net_sales_value  ,

    cast(ap_net_realisable_revenue as number(38,10) ) as ap_net_realisable_revenue  ,

    cast(ap_net_sales_value as number(38,10) ) as ap_net_sales_value  ,

    cast(ap_off_invoice_disc as number(38,10) ) as ap_off_invoice_disc  ,

    cast(ap_off_invoice_disc_mgmt_adjustment as number(38,10) ) as ap_off_invoice_disc_mgmt_adjustment  ,

    cast(ap_off_invoice_disc_pre_adjustment as number(38,10) ) as ap_off_invoice_disc_pre_adjustment  ,

    cast(ap_other_direct_payments as number(38,10) ) as ap_other_direct_payments  ,

    cast(ap_other_direct_payments_mgmt_adjustment as number(38,10) ) as ap_other_direct_payments_mgmt_adjustment  ,

    cast(ap_other_direct_payments_pre_adjustment as number(38,10) ) as ap_other_direct_payments_pre_adjustment  ,

    cast(ap_other_indirect_payments as number(38,10) ) as ap_other_indirect_payments  ,

    cast(ap_other_indirect_payments_mgmt_adjustment as number(38,10) ) as ap_other_indirect_payments_mgmt_adjustment  ,

    cast(ap_other_indirect_payments_pre_adjustment as number(38,10) ) as ap_other_indirect_payments_pre_adjustment  ,

    cast(ap_permanent_disc as number(38,10) ) as ap_permanent_disc  ,

    cast(ap_promo_fixed_funding as number(38,10) ) as ap_promo_fixed_funding  ,

    cast(ap_promo_fixed_funding_mgmt_adjustment as number(38,10) ) as ap_promo_fixed_funding_mgmt_adjustment  ,

    cast(ap_promo_fixed_funding_pre_adjustment as number(38,10) ) as ap_promo_fixed_funding_pre_adjustment  ,

    cast(ap_range_support_allowance as number(38,10) ) as ap_range_support_allowance  ,

    cast(ap_range_support_allowance_mgmt_adjustment as number(38,10) ) as ap_range_support_allowance_mgmt_adjustment  ,

    cast(ap_range_support_allowance_pre_adjustment as number(38,10) ) as ap_range_support_allowance_pre_adjustment  ,

    cast(ap_range_support_incentives as number(38,10) ) as ap_range_support_incentives  ,

    cast(ap_range_support_incentives_mgmt_adjustment as number(38,10) ) as ap_range_support_incentives_mgmt_adjustment  ,

    cast(ap_range_support_incentives_pre_adjustment as number(38,10) ) as ap_range_support_incentives_pre_adjustment  ,

    cast(ap_retail_cost_of_sales as number(38,10) ) as ap_retail_cost_of_sales  ,

    cast(ap_retail_margin_excl_fixed_funding as number(38,10) ) as ap_retail_margin_excl_fixed_funding  ,

    cast(ap_retail_margin_incl_fixed_funding as number(38,10) ) as ap_retail_margin_incl_fixed_funding  ,

    cast(ap_retail_promo_fixed_spend as number(38,10) ) as ap_retail_promo_fixed_spend  ,

    cast(ap_retail_retailer_retro_funding as number(38,10) ) as ap_retail_retailer_retro_funding  ,

    cast(ap_retail_revenue_mrrsp as number(38,10) ) as ap_retail_revenue_mrrsp  ,

    cast(ap_retail_revenue_net as number(38,10) ) as ap_retail_revenue_net  ,

    cast(ap_retail_revenue_net_excl_mrrsp as number(38,10) ) as ap_retail_revenue_net_excl_mrrsp  ,

    cast(ap_retail_revenue_net_excl_rsp as number(38,10) ) as ap_retail_revenue_net_excl_rsp  ,

    cast(ap_retail_revenue_rsp as number(38,10) ) as ap_retail_revenue_rsp  ,

    cast(ap_retail_total_spend as number(38,10) ) as ap_retail_total_spend  ,

    cast(ap_retro as number(38,10) ) as ap_retro  ,

    cast(ap_retro_mgmt_adjustment as number(38,10) ) as ap_retro_mgmt_adjustment  ,

    cast(ap_retro_pre_adjustment as number(38,10) ) as ap_retro_pre_adjustment  ,

    cast(ap_tot_prime_cost_standard as number(38,10) ) as ap_tot_prime_cost_standard  ,

    cast(ap_tot_prime_cost_standard_mgmt_adjustment as number(38,10) ) as ap_tot_prime_cost_standard_mgmt_adjustment  ,

    cast(ap_tot_prime_cost_standard_pre_adjustment as number(38,10) ) as ap_tot_prime_cost_standard_pre_adjustment  ,

    cast(ap_tot_prime_cost_variance as number(38,10) ) as ap_tot_prime_cost_variance  ,

    cast(ap_tot_prime_cost_variance_mgmt_adjustment as number(38,10) ) as ap_tot_prime_cost_variance_mgmt_adjustment  ,

    cast(ap_tot_prime_cost_variance_pre_adjustment as number(38,10) ) as ap_tot_prime_cost_variance_pre_adjustment  ,

    cast(ap_total_trade as number(38,10) ) as ap_total_trade  ,

    cast(ap_total_trade_cust_invoiced as number(38,10) ) as ap_total_trade_cust_invoiced  ,

    cast(ap_variable_trade as number(38,10) ) as ap_variable_trade  ,

    cast(promo_vol as number(38,10) ) as promo_vol  ,

    cast(promo_vol_kg as number(38,10) ) as promo_vol_kg  ,

    cast(promo_vol_ul as number(38,10) ) as promo_vol_ul  ,

    cast(retail_tot_vol_ca as number(38,10) ) as retail_tot_vol_ca  ,

    cast(retail_tot_vol_kg as number(38,10) ) as retail_tot_vol_kg  ,

    cast(retail_tot_vol_sgl as number(38,10) ) as retail_tot_vol_sgl  ,

    cast(retail_tot_vol_sgl_ca as number(38,10) ) as retail_tot_vol_sgl_ca  ,

    cast(retail_tot_vol_sgl_ul as number(38,10) ) as retail_tot_vol_sgl_ul  ,

    cast(retail_tot_vol_sp_base_uom as number(38,10) ) as retail_tot_vol_sp_base_uom  ,

    cast(retail_tot_vol_sp_kg_uom as number(38,10) ) as retail_tot_vol_sp_kg_uom  ,

    cast(retail_tot_vol_sp_ul_uom as number(38,10) ) as retail_tot_vol_sp_ul_uom  ,

    cast(retail_tot_vol_ul as number(38,10) ) as retail_tot_vol_ul  ,

    cast(tot_vol_ca as number(38,10) ) as tot_vol_ca  ,

    cast(tot_vol_kg as number(38,10) ) as tot_vol_kg  ,

    cast(tot_vol_sgl as number(38,10) ) as tot_vol_sgl  ,

    cast(tot_vol_sgl_ca as number(38,10) ) as tot_vol_sgl_ca  ,

    cast(tot_vol_sgl_ul as number(38,10) ) as tot_vol_sgl_ul  ,

    cast(tot_vol_sp_base_uom as number(38,10) ) as tot_vol_sp_base_uom  ,

    cast(tot_vol_sp_base_uom_mgmt_adjustment as number(38,10) ) as tot_vol_sp_base_uom_mgmt_adjustment  ,

    cast(tot_vol_sp_base_uom_pre_adjustment as number(38,10) ) as tot_vol_sp_base_uom_pre_adjustment  ,

    cast(tot_vol_sp_kg_uom as number(38,10) ) as tot_vol_sp_kg_uom  ,

    cast(tot_vol_sp_kg_uom_mgmt_adjustment as number(38,10) ) as tot_vol_sp_kg_uom_mgmt_adjustment  ,

    cast(tot_vol_sp_kg_uom_pre_adjustment as number(38,10) ) as tot_vol_sp_kg_uom_pre_adjustment  ,

    cast(tot_vol_sp_ul_uom as number(38,10) ) as tot_vol_sp_ul_uom  ,

    cast(tot_vol_sp_ul_uom_mgmt_adjustment as number(38,10) ) as tot_vol_sp_ul_uom_mgmt_adjustment  ,

    cast(tot_vol_sp_ul_uom_pre_adjustment as number(38,10) ) as tot_vol_sp_ul_uom_pre_adjustment  ,

    cast(tot_vol_ul as number(38,10) ) as tot_vol_ul  ,

    cast(fcf_tot_vol_kg as number(38,10) ) as fcf_tot_vol_kg  ,

    cast(fcf_tot_vol_ca as number(38,10) ) as fcf_tot_vol_ca  ,

    cast(fcf_tot_vol_ul as number(38,10) ) as fcf_tot_vol_ul  ,

    cast(fcf_base_vol_kg as number(38,10) ) as fcf_base_vol_kg  ,

    cast(fcf_base_vol_ca as number(38,10) ) as fcf_base_vol_ca  ,

    cast(fcf_base_vol_ul as number(38,10) ) as fcf_base_vol_ul  ,

    cast(fcf_promo_vol_kg as number(38,10) ) as fcf_promo_vol_kg  ,

    cast(fcf_promo_vol_ca as number(38,10) ) as fcf_promo_vol_ca  ,

    cast(fcf_promo_vol_ul as number(38,10) ) as fcf_promo_vol_ul  ,

    cast(fcf_over_vol_kg as number(38,10) ) as fcf_over_vol_kg  ,

    cast(fcf_over_vol_ca as number(38,10) ) as fcf_over_vol_ca  ,

    cast(fcf_over_vol_ul as number(38,10) ) as fcf_over_vol_ul  ,

    cast(gl_unit_price as float)  as gl_unit_price  ,

    cast(nvl(raw_material_unit_price,0) as float)  as raw_material_unit_price  ,

    cast(nvl(ap_tot_prime_cost_standard_raw,0) as float)  as ap_tot_prime_cost_standard_raw  ,

    cast(nvl(packaging_unit_price,0) as float)  as packaging_unit_price  ,

    cast(nvl(ap_tot_prime_cost_standard_packaging,0) as float)  as ap_tot_prime_cost_standard_packaging  ,

    cast(nvl(labour_unit_price,0) as float)  as labour_unit_price  ,

    cast(nvl(ap_tot_prime_cost_standard_labour,0) as float)  as ap_tot_prime_cost_standard_labour  ,

    cast(nvl(bought_in_unit_price,0) as float)  as bought_in_unit_price  ,

    cast(nvl(ap_tot_prime_cost_standard_bought_in,0) as float)  as ap_tot_prime_cost_standard_bought_in  ,

    cast(nvl(other_unit_price,0) as float) as other_unit_price  ,

    cast(nvl(ap_tot_prime_cost_standard_other,0) as float)  as ap_tot_prime_cost_standard_other  ,

    cast(nvl(co_pack_unit_price,0) as float)  as co_pack_unit_price  ,

    cast(nvl(ap_tot_prime_cost_standard_co_pack,0) as float)  as ap_tot_prime_cost_standard_co_pack  

from final
