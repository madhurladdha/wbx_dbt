{{
    config(
        tags = ["sls","sales","forecast","sls_forecast","sls_finance"]
    )
}}
with account_plan_actual as (
    select * from {{ ref('src_exc_fact_account_plan_actual')}}
),
customer as (
    select * from {{ ref('src_exc_dim_pc_customer')}}
),
product as (
    select * from {{ ref('src_exc_dim_pc_product')}}
),
scenario as (
    select * from {{ ref('src_exc_dim_scenario')}}
),
snapshot_date as (
    select snapshot_Date from (
    select *,rank() over (order by snapshot_Date desc) rnknum 
    from {{ ref('stg_d_wtx_lkp_snapshot_date')}} ) where rnknum=1
),
planning_date as (
    select * from {{ ref('dim_wbx_planning_date_oc')}}
),
final as (
    select
      
    '{{env_var("DBT_SOURCE_SYSTEM")}}'  as source_system,
    sku_idx,
    prod.code as source_item_identifier,
    cust_idx,
    cust.code as plan_source_customer_code,
    to_date(to_char(day_idx),'YYYYMMDD')AS calendar_date,
    fact.scen_idx as scen_idx,
    'HISTORY' as scen_code,
    'HISTORY' as scen_name,
    isonpromo_si,
    isonpromo_so,
    tot_vol_sp_base_uom,
    tot_vol_sp_base_uom_pre_adjustment,
    tot_vol_sp_base_uom_mgmt_adjustment,
    retail_tot_vol_sp_base_uom,
    promo_vol,
tot_vol_sgl,
tot_vol_kg,
ap_added_value_pack,
ap_permanent_disc,
ap_invoiced_sales_value,
ap_net_sales_value,
ap_net_realisable_revenue,
ap_variable_trade,
ap_net_net_sales_value,
ap_fixed_trade_cust_invoiced,
ap_total_trade_cust_invoiced,
ap_fixed_trade_non_cust_invoiced,
ap_total_trade,
ap_gross_margin_standard,
ap_gross_margin_actual,
ap_gcat_standard,
ap_gcat_actuals,
ap_fixed_annual_payments_pre_adjustment,
ap_fixed_annual_payments_mgmt_adjustment,
ap_fixed_annual_payments,
ap_category_pre_adjustment,
ap_category_mgmt_adjustment,
ap_category,
ap_promo_fixed_funding_pre_adjustment,
ap_promo_fixed_funding_mgmt_adjustment,
ap_promo_fixed_funding,
ap_cash_disc_pre_adjustment,
ap_cash_disc_mgmt_adjustment,
ap_cash_disc,
ap_direct_shopper_marketing_pre_adjustment,
ap_direct_shopper_marketing_mgmt_adjustment,
ap_direct_shopper_marketing,
ap_range_support_allowance_pre_adjustment,
ap_range_support_allowance_mgmt_adjustment,
ap_range_support_allowance,
ap_range_support_incentives_pre_adjustment,
ap_range_support_incentives_mgmt_adjustment,
ap_range_support_incentives,
ap_indirect_shopper_marketing_pre_adjustment,
ap_indirect_shopper_marketing_mgmt_adjustment,
ap_indirect_shopper_marketing,
ap_retro_pre_adjustment,
ap_retro_mgmt_adjustment,
ap_retro,
ap_avp_disc_pre_adjustment,
ap_avp_disc_mgmt_adjustment,
ap_avp_disc,
ap_everyday_low_prices_pre_adjustment,
ap_everyday_low_prices_mgmt_adjustment,
ap_everyday_low_prices,
ap_off_invoice_disc_pre_adjustment,
ap_off_invoice_disc_mgmt_adjustment,
ap_off_invoice_disc,
ap_field_marketing_pre_adjustment,
ap_field_marketing_mgmt_adjustment,
ap_field_marketing,
ap_tot_prime_cost_variance_pre_adjustment,
ap_tot_prime_cost_variance_mgmt_adjustment,
ap_tot_prime_cost_variance,
ap_tot_prime_cost_standard_pre_adjustment,
ap_tot_prime_cost_standard_mgmt_adjustment,
ap_tot_prime_cost_standard,
ap_early_settlement_disc_pre_adjustment,
ap_early_settlement_disc_mgmt_adjustment,
ap_early_settlement_disc,
ap_other_direct_payments_pre_adjustment,
ap_other_direct_payments_mgmt_adjustment,
ap_other_direct_payments,
ap_other_indirect_payments_pre_adjustment,
ap_other_indirect_payments_mgmt_adjustment,
ap_other_indirect_payments,
ap_gross_selling_value_pre_adjustment,
ap_gross_selling_value_mgmt_adjustment,
ap_gross_selling_value,
ap_gross_sales_value,
ap_growth_incentives_pre_adjustment,
ap_growth_incentives_mgmt_adjustment,
ap_growth_incentives,
retail_tot_vol_sgl,
retail_tot_vol_kg,
ap_retail_revenue_mrrsp,
ap_retail_revenue_rsp,
ap_retail_revenue_net,
ap_retail_cost_of_sales,
ap_retail_retailer_retro_funding,
ap_retail_margin_excl_fixed_funding,
ap_retail_promo_fixed_spend,
ap_retail_total_spend,
ap_retail_margin_incl_fixed_funding,
ap_retail_revenue_net_excl_mrrsp,
ap_retail_revenue_net_excl_rsp
from account_plan_actual fact

left outer join scenario scen
on fact.scen_idx = scen.scen_idx
left outer join customer cust 
on fact.cust_idx = cust.idx 
left outer join product prod
on fact.sku_idx = prod.idx
left outer join planning_date dt 
on to_date(to_char(fact.day_idx),'yyyymmdd')=dt.calendar_date
and dt.source_system='{{env_var("DBT_SOURCE_SYSTEM")}}' 
LEFT JOIN snapshot_date snapshot on 1=1
where fact.scen_idx = 1
and to_date(to_char(day_idx),'YYYYMMDD') between (select dateadd(month,-1,(select date_trunc('month', snapshot.snapshot_date)) )) and last_day(snapshot.snapshot_date)
and ( ( tot_vol_sp_base_uom <> 0 and tot_vol_sp_base_uom is not null ) or 
( tot_vol_sp_base_uom_pre_adjustment <> 0 and tot_vol_sp_base_uom_pre_adjustment is not null ) or 
( tot_vol_sp_base_uom_mgmt_adjustment <> 0 and tot_vol_sp_base_uom_mgmt_adjustment is not null ) or 
( retail_tot_vol_sp_base_uom <> 0 and retail_tot_vol_sp_base_uom is not null ) or 
( promo_vol <> 0 and promo_vol is not null ) or 
( tot_vol_sgl <> 0 and tot_vol_sgl is not null ) or 
( tot_vol_kg <> 0 and tot_vol_kg is not null ) or 
( ap_added_value_pack <> 0 and ap_added_value_pack is not null ) or 
( ap_permanent_disc <> 0 and ap_permanent_disc is not null ) or 
( ap_invoiced_sales_value <> 0 and ap_invoiced_sales_value is not null ) or 
( ap_net_sales_value <> 0 and ap_net_sales_value is not null ) or 
( ap_net_realisable_revenue <> 0 and ap_net_realisable_revenue is not null ) or 
( ap_variable_trade <> 0 and ap_variable_trade is not null ) or 
( ap_net_net_sales_value <> 0 and ap_net_net_sales_value is not null ) or 
( ap_fixed_trade_cust_invoiced <> 0 and ap_fixed_trade_cust_invoiced is not null ) or 
( ap_total_trade_cust_invoiced <> 0 and ap_total_trade_cust_invoiced is not null ) or 
( ap_fixed_trade_non_cust_invoiced <> 0 and ap_fixed_trade_non_cust_invoiced is not null ) or 
( ap_total_trade <> 0 and ap_total_trade is not null ) or 
( ap_gross_margin_standard <> 0 and ap_gross_margin_standard is not null ) or 
( ap_gross_margin_actual <> 0 and ap_gross_margin_actual is not null ) or 
( ap_gcat_standard <> 0 and ap_gcat_standard is not null ) or 
( ap_gcat_actuals <> 0 and ap_gcat_actuals is not null ) or 
( ap_fixed_annual_payments_pre_adjustment <> 0 and ap_fixed_annual_payments_pre_adjustment is not null ) or 
( ap_fixed_annual_payments_mgmt_adjustment <> 0 and ap_fixed_annual_payments_mgmt_adjustment is not null ) or 
( ap_fixed_annual_payments <> 0 and ap_fixed_annual_payments is not null ) or 
( ap_category_pre_adjustment <> 0 and ap_category_pre_adjustment is not null ) or 
( ap_category_mgmt_adjustment <> 0 and ap_category_mgmt_adjustment is not null ) or 
( ap_category <> 0 and ap_category is not null ) or 
( ap_promo_fixed_funding_pre_adjustment <> 0 and ap_promo_fixed_funding_pre_adjustment is not null ) or 
( ap_promo_fixed_funding_mgmt_adjustment <> 0 and ap_promo_fixed_funding_mgmt_adjustment is not null ) or 
( ap_promo_fixed_funding <> 0 and ap_promo_fixed_funding is not null ) or 
( ap_cash_disc_pre_adjustment <> 0 and ap_cash_disc_pre_adjustment is not null ) or 
( ap_cash_disc_mgmt_adjustment <> 0 and ap_cash_disc_mgmt_adjustment is not null ) or 
( ap_cash_disc <> 0 and ap_cash_disc is not null ) or 
( ap_direct_shopper_marketing_pre_adjustment <> 0 and ap_direct_shopper_marketing_pre_adjustment is not null ) or 
( ap_direct_shopper_marketing_mgmt_adjustment <> 0 and ap_direct_shopper_marketing_mgmt_adjustment is not null ) or 
( ap_direct_shopper_marketing <> 0 and ap_direct_shopper_marketing is not null ) or 
( ap_range_support_allowance_pre_adjustment <> 0 and ap_range_support_allowance_pre_adjustment is not null ) or 
( ap_range_support_allowance_mgmt_adjustment <> 0 and ap_range_support_allowance_mgmt_adjustment is not null ) or 
( ap_range_support_allowance <> 0 and ap_range_support_allowance is not null ) or 
( ap_range_support_incentives_pre_adjustment <> 0 and ap_range_support_incentives_pre_adjustment is not null ) or 
( ap_range_support_incentives_mgmt_adjustment <> 0 and ap_range_support_incentives_mgmt_adjustment is not null ) or 
( ap_range_support_incentives <> 0 and ap_range_support_incentives is not null ) or 
( ap_indirect_shopper_marketing_pre_adjustment <> 0 and ap_indirect_shopper_marketing_pre_adjustment is not null ) or 
( ap_indirect_shopper_marketing_mgmt_adjustment <> 0 and ap_indirect_shopper_marketing_mgmt_adjustment is not null ) or 
( ap_indirect_shopper_marketing <> 0 and ap_indirect_shopper_marketing is not null ) or 
( ap_retro_pre_adjustment <> 0 and ap_retro_pre_adjustment is not null ) or 
( ap_retro_mgmt_adjustment <> 0 and ap_retro_mgmt_adjustment is not null ) or 
( ap_retro <> 0 and ap_retro is not null ) or 
( ap_avp_disc_pre_adjustment <> 0 and ap_avp_disc_pre_adjustment is not null ) or 
( ap_avp_disc_mgmt_adjustment <> 0 and ap_avp_disc_mgmt_adjustment is not null ) or 
( ap_avp_disc <> 0 and ap_avp_disc is not null ) or 
( ap_everyday_low_prices_pre_adjustment <> 0 and ap_everyday_low_prices_pre_adjustment is not null ) or 
( ap_everyday_low_prices_mgmt_adjustment <> 0 and ap_everyday_low_prices_mgmt_adjustment is not null ) or 
( ap_everyday_low_prices <> 0 and ap_everyday_low_prices is not null ) or 
( ap_off_invoice_disc_pre_adjustment <> 0 and ap_off_invoice_disc_pre_adjustment is not null ) or 
( ap_off_invoice_disc_mgmt_adjustment <> 0 and ap_off_invoice_disc_mgmt_adjustment is not null ) or 
( ap_off_invoice_disc <> 0 and ap_off_invoice_disc is not null ) or 
( ap_field_marketing_pre_adjustment <> 0 and ap_field_marketing_pre_adjustment is not null ) or 
( ap_field_marketing_mgmt_adjustment <> 0 and ap_field_marketing_mgmt_adjustment is not null ) or 
( ap_field_marketing <> 0 and ap_field_marketing is not null ) or 
( ap_tot_prime_cost_variance_pre_adjustment <> 0 and ap_tot_prime_cost_variance_pre_adjustment is not null ) or 
( ap_tot_prime_cost_variance_mgmt_adjustment <> 0 and ap_tot_prime_cost_variance_mgmt_adjustment is not null ) or 
( ap_tot_prime_cost_variance <> 0 and ap_tot_prime_cost_variance is not null ) or 
( ap_tot_prime_cost_standard_pre_adjustment <> 0 and ap_tot_prime_cost_standard_pre_adjustment is not null ) or 
( ap_tot_prime_cost_standard_mgmt_adjustment <> 0 and ap_tot_prime_cost_standard_mgmt_adjustment is not null ) or 
( ap_tot_prime_cost_standard <> 0 and ap_tot_prime_cost_standard is not null ) or 
( ap_early_settlement_disc_pre_adjustment <> 0 and ap_early_settlement_disc_pre_adjustment is not null ) or 
( ap_early_settlement_disc_mgmt_adjustment <> 0 and ap_early_settlement_disc_mgmt_adjustment is not null ) or 
( ap_early_settlement_disc <> 0 and ap_early_settlement_disc is not null ) or 
( ap_other_direct_payments_pre_adjustment <> 0 and ap_other_direct_payments_pre_adjustment is not null ) or 
( ap_other_direct_payments_mgmt_adjustment <> 0 and ap_other_direct_payments_mgmt_adjustment is not null ) or 
( ap_other_direct_payments <> 0 and ap_other_direct_payments is not null ) or 
( ap_other_indirect_payments_pre_adjustment <> 0 and ap_other_indirect_payments_pre_adjustment is not null ) or 
( ap_other_indirect_payments_mgmt_adjustment <> 0 and ap_other_indirect_payments_mgmt_adjustment is not null ) or 
( ap_other_indirect_payments <> 0 and ap_other_indirect_payments is not null ) or 
( ap_gross_selling_value_pre_adjustment <> 0 and ap_gross_selling_value_pre_adjustment is not null ) or 
( ap_gross_selling_value_mgmt_adjustment <> 0 and ap_gross_selling_value_mgmt_adjustment is not null ) or 
( ap_gross_selling_value <> 0 and ap_gross_selling_value is not null ) or 
( ap_gross_sales_value <> 0 and ap_gross_sales_value is not null ) or 
( ap_growth_incentives_pre_adjustment <> 0 and ap_growth_incentives_pre_adjustment is not null ) or 
( ap_growth_incentives_mgmt_adjustment <> 0 and ap_growth_incentives_mgmt_adjustment is not null ) or 
( ap_growth_incentives <> 0 and ap_growth_incentives is not null ) or 
( retail_tot_vol_sgl <> 0 and retail_tot_vol_sgl is not null ) or 
( retail_tot_vol_kg <> 0 and retail_tot_vol_kg is not null ) or 
( ap_retail_revenue_mrrsp <> 0 and ap_retail_revenue_mrrsp is not null ) or 
( ap_retail_revenue_rsp <> 0 and ap_retail_revenue_rsp is not null ) or 
( ap_retail_revenue_net <> 0 and ap_retail_revenue_net is not null ) or 
( ap_retail_cost_of_sales <> 0 and ap_retail_cost_of_sales is not null ) or 
( ap_retail_retailer_retro_funding <> 0 and ap_retail_retailer_retro_funding is not null ) or 
( ap_retail_margin_excl_fixed_funding <> 0 and ap_retail_margin_excl_fixed_funding is not null ) or 
( ap_retail_promo_fixed_spend <> 0 and ap_retail_promo_fixed_spend is not null ) or 
( ap_retail_total_spend <> 0 and ap_retail_total_spend is not null ) or 
( ap_retail_margin_incl_fixed_funding <> 0 and ap_retail_margin_incl_fixed_funding is not null ) or 
( ap_retail_revenue_net_excl_mrrsp <> 0 and ap_retail_revenue_net_excl_mrrsp is not null ) or 
( ap_retail_revenue_net_excl_rsp <> 0 and ap_retail_revenue_net_excl_rsp is not null ))
)
select * from final
