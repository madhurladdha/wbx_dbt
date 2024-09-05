

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_PandL_PromoSkuDay') }}

),

renamed as (

    select
        promo_idx,
        cust_idx,
        sku_idx,
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
        t_ap_retail_revenue_net_excl_rsp

    from source

)

select * from renamed
