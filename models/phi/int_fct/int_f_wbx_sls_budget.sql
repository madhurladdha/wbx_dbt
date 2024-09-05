{{
    config(
        tags=["sales", "budget", "sls_budget", "sls_budget_fin", "adhoc"],
    )
}}
{% set sign_flip = 1 %}
with
    fact as (
        select *
        from {{ ref("fct_wbx_sls_budget_fin") }}
        where
            frozen_forecast in (
                select distinct frozen_forecast
                from {{ ref("stg_f_wbx_sls_budget_fin") }}
            )
    ),
    item_ext as (
        select
            source_system,
            source_item_identifier,
            max(dummy_product_flag) as dummy_product_flag
        from {{ ref("dim_wbx_item_ext") }}
        group by source_system, source_item_identifier
    ),
    pass1_calcs as (
        select
            old_budget.calendar_date as calendar_date,
            to_char(old_budget.item_guid) as item_guid,
            0 as plan_customer_addr_number_guid,
            old_budget.trade_type_code as plan_source_customer_code,
            old_budget.frozen_forecast as frozen_forecast,
            old_budget.source_item_identifier as source_item_identifier,
            (old_budget.pif_isa_amt * {{ sign_flip }})
            + (old_budget.mif_isa_amt * {{ sign_flip }})
            + (
                old_budget.pif_trade_enh_amt * {{ sign_flip }}
            ) as ap_fixed_trade_cust_invoiced,
            (mif_customer_mktg_amt * {{ sign_flip }})
            + (mif_category_amt * {{ sign_flip }})
            + 0
            + (mif_field_mktg_amt * {{ sign_flip }})
            + (
                mif_range_support_incent_amt * {{ sign_flip }}
            ) as ap_fixed_trade_non_cust_invoiced,
            old_budget.gross_value_amt
            + (old_budget.pif_trade_avp_amt * {{ sign_flip }}) as ap_gross_sales_value,
            (old_budget.rsa_amt * {{ sign_flip }})
            + (old_budget.edlp_amt * {{ sign_flip }})
            + (old_budget.settlement_amt * {{ sign_flip }})
            + (old_budget.gincent_amt * {{ sign_flip }})
            + (
                old_budget.back_margin_amt * {{ sign_flip }}
            ) as intermediate_permanent_disc,
            (old_budget.boughtin_cost_amt * {{ sign_flip }})
            + (old_budget.copack_cost_amt * {{ sign_flip }})
            + (old_budget.rawmats_cost_amt * {{ sign_flip }})
            + (old_budget.labour_cost_amt * {{ sign_flip }})
            + (old_budget.rye_adj_cost_amt * {{ sign_flip }})
            + (old_budget.pack_cost_amt * {{ sign_flip }}) as intermediate_total_pcos,
            old_budget.gross_value_amt + (
                old_budget.pif_trade_avp_amt * {{ sign_flip }}
            ) as ap_net_net_sales_value,
            (
                (
                    old_budget.exp_trade_spend_amt
                    + old_budget.pif_trade_red_amt
                    + old_budget.pif_trade_oib_amt
                )
                * {{ sign_flip }}
            )
            + (old_budget.pif_trade_avp_amt * {{ sign_flip }}) as ap_variable_trade
        from fact old_budget
    ),
    pass2_calcs as (
        select
            old_budget.calendar_date as calendar_date,
            to_char(old_budget.item_guid) as item_guid,
            0 as plan_customer_addr_number_guid,
            old_budget.trade_type_code as plan_source_customer_code,
            old_budget.frozen_forecast as frozen_forecast,
            old_budget.source_item_identifier as source_item_identifier,
            -- PASS 1 CALCS
            calcs.ap_fixed_trade_cust_invoiced as ap_fixed_trade_cust_invoiced,
            calcs.ap_fixed_trade_non_cust_invoiced as ap_fixed_trade_non_cust_invoiced,
            calcs.ap_gross_sales_value as ap_gross_sales_value,
            calcs.intermediate_permanent_disc as intermediate_permanent_disc,
            calcs.intermediate_total_pcos as intermediate_total_pcos,
            calcs.ap_net_net_sales_value as ap_net_net_sales_value,
            calcs.ap_variable_trade as ap_variable_trade,
            -- PASS 2 CALCS
            calcs.ap_gross_sales_value
            - intermediate_permanent_disc as ap_net_sales_value,
            calcs.ap_variable_trade
            + calcs.ap_fixed_trade_cust_invoiced
            + ap_fixed_trade_non_cust_invoiced as ap_total_trade,
            calcs.ap_variable_trade
            + calcs.ap_fixed_trade_cust_invoiced as ap_total_trade_cust_invoiced
        from fact old_budget
        join
            pass1_calcs calcs
            on nvl(old_budget.calendar_date, '01-JAN-2000')
            = nvl(calcs.calendar_date, '01-JAN-2000')
            and nvl(to_char(old_budget.item_guid), 'X')
            = nvl(to_char(calcs.item_guid), 'X')
            and nvl(upper(trim(old_budget.trade_type_code)), 'X')
            = nvl(upper(trim(calcs.plan_source_customer_code)), 'X')
            and nvl(upper(trim(old_budget.frozen_forecast)), 'X')
            = nvl(upper(trim(calcs.frozen_forecast)), 'X')
            and nvl(upper(trim(old_budget.source_item_identifier)), 'X')
            = nvl(upper(trim(calcs.source_item_identifier)), 'X')
    ),
    pass3_calcs as (
        select
            old_budget.calendar_date as calendar_date,
            to_char(old_budget.item_guid) as item_guid,
            0 as plan_customer_addr_number_guid,
            old_budget.trade_type_code as plan_source_customer_code,
            old_budget.frozen_forecast as frozen_forecast,
            old_budget.source_item_identifier as source_item_identifier,
            -- PASS 1 CALCS
            calcs.ap_fixed_trade_cust_invoiced as ap_fixed_trade_cust_invoiced,
            calcs.ap_fixed_trade_non_cust_invoiced as ap_fixed_trade_non_cust_invoiced,
            calcs.ap_gross_sales_value as ap_gross_sales_value,
            calcs.intermediate_permanent_disc as intermediate_permanent_disc,
            calcs.intermediate_total_pcos as intermediate_total_pcos,
            calcs.ap_net_net_sales_value as ap_net_net_sales_value,
            calcs.ap_variable_trade as ap_variable_trade,
            -- PASS 2 CALCS
            calcs.ap_net_sales_value as ap_net_sales_value,
            calcs.ap_total_trade as ap_total_trade,
            calcs.ap_total_trade_cust_invoiced as ap_total_trade_cust_invoiced,
            -- PASS 3 CALCS
            calcs.ap_net_sales_value
            - calcs.ap_total_trade_cust_invoiced as ap_net_realisable_revenue
        from fact old_budget
        join
            pass2_calcs calcs
            on nvl(old_budget.calendar_date, '01-JAN-2000')
            = nvl(calcs.calendar_date, '01-JAN-2000')
            and nvl(to_char(old_budget.item_guid), 'X')
            = nvl(to_char(calcs.item_guid), 'X')
            and nvl(upper(trim(old_budget.trade_type_code)), 'X')
            = nvl(upper(trim(calcs.plan_source_customer_code)), 'X')
            and nvl(upper(trim(old_budget.frozen_forecast)), 'X')
            = nvl(upper(trim(calcs.frozen_forecast)), 'X')
            and nvl(upper(trim(old_budget.source_item_identifier)), 'X')
            = nvl(upper(trim(calcs.source_item_identifier)), 'X')
    ),
    pass4_calcs as (
        select
            old_budget.calendar_date as calendar_date,
            to_char(old_budget.item_guid) as item_guid,
            0 as plan_customer_addr_number_guid,
            old_budget.trade_type_code as plan_source_customer_code,
            old_budget.frozen_forecast as frozen_forecast,
            old_budget.source_item_identifier as source_item_identifier,
            -- PASS 1 CALCS
            calcs.ap_fixed_trade_cust_invoiced as ap_fixed_trade_cust_invoiced,
            calcs.ap_fixed_trade_non_cust_invoiced as ap_fixed_trade_non_cust_invoiced,
            calcs.ap_gross_sales_value as ap_gross_sales_value,
            calcs.intermediate_permanent_disc as intermediate_permanent_disc,
            calcs.intermediate_total_pcos as intermediate_total_pcos,
            calcs.ap_net_net_sales_value as ap_net_net_sales_value,
            calcs.ap_variable_trade as ap_variable_trade,
            -- PASS 2 CALCS
            calcs.ap_net_sales_value as ap_net_sales_value,
            calcs.ap_total_trade as ap_total_trade,
            calcs.ap_total_trade_cust_invoiced as ap_total_trade_cust_invoiced,
            -- PASS 3 CALCS
            calcs.ap_net_realisable_revenue as ap_net_realisable_revenue,
            -- PASS 4 CALCS
            calcs.ap_net_realisable_revenue
            - calcs.intermediate_total_pcos as ap_gross_margin_actual,
            calcs.ap_net_realisable_revenue
            - calcs.intermediate_total_pcos as ap_gross_margin_standard
        from fact old_budget
        join
            pass3_calcs calcs
            on nvl(old_budget.calendar_date, '01-JAN-2000')
            = nvl(calcs.calendar_date, '01-JAN-2000')
            and nvl(to_char(old_budget.item_guid), 'X')
            = nvl(to_char(calcs.item_guid), 'X')
            and nvl(upper(trim(old_budget.trade_type_code)), 'X')
            = nvl(upper(trim(calcs.plan_source_customer_code)), 'X')
            and nvl(upper(trim(old_budget.frozen_forecast)), 'X')
            = nvl(upper(trim(calcs.frozen_forecast)), 'X')
            and nvl(upper(trim(old_budget.source_item_identifier)), 'X')
            = nvl(upper(trim(calcs.source_item_identifier)), 'X')
    ),
    fct_wbx_sls_budget as (
        select
            old_budget.calendar_date as calendar_date,
            null as is_vol_total_nonzero,
            null as isonpromo_si,
            null as isonpromo_so,
            null as ispreorpostpromo_si,
            null as ispreorpostpromo_so,
            to_char(old_budget.item_guid) as item_guid,
            null as listingactive,
            0 as plan_customer_addr_number_guid,
            old_budget.trade_type_code as plan_source_customer_code,
            null as qty_ca_cannib_loss_si,
            null as qty_ca_cannib_loss_so,
            null as qty_ca_effective_base_fc_si,
            null as qty_ca_effective_base_fc_so,
            null as qty_ca_override_si,
            null as qty_ca_override_so,
            null as qty_ca_pp_dip_si,
            null as qty_ca_pp_dip_so,
            null as qty_ca_promo_total_si,
            null as qty_ca_promo_total_so,
            null as qty_ca_si_actual,
            null as qty_ca_so_actual,
            null as qty_ca_stat_base_fc_si,
            null as qty_ca_stat_base_fc_so,
            null as qty_ca_total_adjust_si,
            null as qty_ca_total_adjust_so,
            null as qty_ca_total_si,
            null as qty_ca_total_so,
            null as qty_kg_cannib_loss_si,
            null as qty_kg_cannib_loss_so,
            null as qty_kg_effective_base_fc_si,
            null as qty_kg_effective_base_fc_so,
            null as qty_kg_override_si,
            null as qty_kg_override_so,
            null as qty_kg_pp_dip_si,
            null as qty_kg_pp_dip_so,
            null as qty_kg_promo_total_si,
            null as qty_kg_promo_total_so,
            null as qty_kg_si_actual,
            null as qty_kg_so_actual,
            null as qty_kg_stat_base_fc_si,
            null as qty_kg_stat_base_fc_so,
            null as qty_kg_total_adjust_si,
            null as qty_kg_total_adjust_so,
            null as qty_kg_total_si,
            null as qty_kg_total_so,
            null as qty_ul_cannib_loss_si,
            null as qty_ul_cannib_loss_so,
            null as qty_ul_effective_base_fc_si,
            null as qty_ul_effective_base_fc_so,
            null as qty_ul_override_si,
            null as qty_ul_override_so,
            null as qty_ul_pp_dip_si,
            null as qty_ul_pp_dip_so,
            null as qty_ul_promo_total_si,
            null as qty_ul_promo_total_so,
            null as qty_ul_si_actual,
            null as qty_ul_so_actual,
            null as qty_ul_stat_base_fc_si,
            null as qty_ul_stat_base_fc_so,
            null as qty_ul_total_adjust_si,
            null as qty_ul_total_adjust_so,
            null as qty_ul_total_si,
            null as qty_ul_total_so,
            null as scen_code,
            null as scenario_guid,
            old_budget.frozen_forecast as frozen_forecast,
            null as snapshot_date,
            old_budget.source_item_identifier as source_item_identifier,
            old_budget.source_system as source_system,
            null as total_baseretentionpercentage,
            null as total_si_preorpostdippercentage,
            null as total_so_preorpostdippercentage,
            pif_trade_avp_amt as ap_added_value_pack,
            old_budget.pif_trade_avp_amt * {{ sign_flip }} as ap_avp_disc,
            null as ap_avp_disc_mgmt_adjustment,
            null as ap_avp_disc_pre_adjustment,
            null as ap_cash_disc,
            null as ap_cash_disc_mgmt_adjustment,
            null as ap_cash_disc_pre_adjustment,
            old_budget.mif_category_amt * {{ sign_flip }} as ap_category,
            null as ap_category_mgmt_adjustment,
            null as ap_category_pre_adjustment,
            null as ap_direct_shopper_marketing,
            null as ap_direct_shopper_marketing_mgmt_adjustment,
            null as ap_direct_shopper_marketing_pre_adjustment,
            old_budget.settlement_amt * {{ sign_flip }} as ap_early_settlement_disc,
            null as ap_early_settlement_disc_mgmt_adjustment,
            null as ap_early_settlement_disc_pre_adjustment,
            old_budget.edlp_amt * {{ sign_flip }} as ap_everyday_low_prices,
            null as ap_everyday_low_prices_mgmt_adjustment,
            null as ap_everyday_low_prices_pre_adjustment,
            old_budget.mif_field_mktg_amt * {{ sign_flip }} as ap_field_marketing,
            null as ap_field_marketing_mgmt_adjustment,
            null as ap_field_marketing_pre_adjustment,
            old_budget.mif_isa_amt * {{ sign_flip }} as ap_fixed_annual_payments,
            null as ap_fixed_annual_payments_mgmt_adjustment,
            null as ap_fixed_annual_payments_pre_adjustment,
            calcs.ap_fixed_trade_cust_invoiced as ap_fixed_trade_cust_invoiced,
            calcs.ap_fixed_trade_non_cust_invoiced as ap_fixed_trade_non_cust_invoiced,
            null as ap_gcat_actuals,
            null as ap_gcat_standard,
            calcs.ap_gross_margin_actual as ap_gross_margin_actual,
            calcs.ap_gross_margin_standard as ap_gross_margin_standard,
            calcs.ap_gross_sales_value as ap_gross_sales_value,
            old_budget.gross_value_amt as ap_gross_selling_value,
            null as ap_gross_selling_value_mgmt_adjustment,
            null as ap_gross_selling_value_pre_adjustment,
            old_budget.back_margin_amt * {{ sign_flip }} as ap_growth_incentives,
            null as ap_growth_incentives_mgmt_adjustment,
            null as ap_growth_incentives_pre_adjustment,
            old_budget.mif_customer_mktg_amt
            * {{ sign_flip }} as ap_indirect_shopper_marketing,
            null as ap_indirect_shopper_marketing_mgmt_adjustment,
            null as ap_indirect_shopper_marketing_pre_adjustment,
            null as ap_invoiced_sales_value,
            null as ap_net_net_sales_value,
            calcs.ap_net_realisable_revenue as ap_net_realisable_revenue,
            calcs.ap_net_sales_value as ap_net_sales_value,
            null as ap_off_invoice_disc,
            null as ap_off_invoice_disc_mgmt_adjustment,
            null as ap_off_invoice_disc_pre_adjustment,
            old_budget.pif_trade_enh_amt * {{ sign_flip }} as ap_other_direct_payments,
            null as ap_other_direct_payments_mgmt_adjustment,
            null as ap_other_direct_payments_pre_adjustment,
            null as ap_other_indirect_payments,
            null as ap_other_indirect_payments_mgmt_adjustment,
            null as ap_other_indirect_payments_pre_adjustment,
            old_budget.rsa_amt * {{ sign_flip }} as ap_permanent_disc,
            old_budget.pif_isa_amt * {{ sign_flip }} as ap_promo_fixed_funding,
            null as ap_promo_fixed_funding_mgmt_adjustment,
            null as ap_promo_fixed_funding_pre_adjustment,
            null as ap_range_support_allowance,
            null as ap_range_support_allowance_mgmt_adjustment,
            null as ap_range_support_allowance_pre_adjustment,
            old_budget.gincent_amt * {{ sign_flip }} as ap_range_support_incentives,
            null as ap_range_support_incentives_mgmt_adjustment,
            null as ap_range_support_incentives_pre_adjustment,
            null as ap_retail_cost_of_sales,
            null as ap_retail_margin_excl_fixed_funding,
            null as ap_retail_margin_incl_fixed_funding,
            null as ap_retail_promo_fixed_spend,
            null as ap_retail_retailer_retro_funding,
            null as ap_retail_revenue_mrrsp,
            null as ap_retail_revenue_net,
            null as ap_retail_revenue_net_excl_mrrsp,
            null as ap_retail_revenue_net_excl_rsp,
            null as ap_retail_revenue_rsp,
            null as ap_retail_total_spend,
            (
                old_budget.exp_trade_spend_amt
                + old_budget.pif_trade_red_amt
                + old_budget.pif_trade_oib_amt
            )
            * {{ sign_flip }} ap_retro,
            null as ap_retro_mgmt_adjustment,
            null as ap_retro_pre_adjustment,
            case
                when item_ext.dummy_product_flag = '0'
                then old_budget.total_cost_amt * {{ sign_flip }}
                else 0
            end as ap_tot_prime_cost_standard,
            null as ap_tot_prime_cost_standard_mgmt_adjustment,
            null as ap_tot_prime_cost_standard_pre_adjustment,
            case
                when item_ext.dummy_product_flag = '1'
                then old_budget.total_cost_amt * {{ sign_flip }}
                else 0
            end as ap_tot_prime_cost_variance,
            null as ap_tot_prime_cost_variance_mgmt_adjustment,
            null as ap_tot_prime_cost_variance_pre_adjustment,
            calcs.ap_total_trade as ap_total_trade,
            calcs.ap_total_trade_cust_invoiced as ap_total_trade_cust_invoiced,
            calcs.ap_variable_trade as ap_variable_trade,
            null as promo_vol,
            null as promo_vol_kg,
            null as promo_vol_ul,
            null as retail_tot_vol_ca,
            null as retail_tot_vol_kg,
            null as retail_tot_vol_sgl,
            null as retail_tot_vol_sgl_ca,
            null as retail_tot_vol_sgl_ul,
            null as retail_tot_vol_sp_base_uom,
            null as retail_tot_vol_sp_kg_uom,
            null as retail_tot_vol_sp_ul_uom,
            null as retail_tot_vol_ul,
            old_budget.budget_qty_ca as tot_vol_ca,
            old_budget.budget_qty_kg as tot_vol_kg,
            null as tot_vol_sgl,
            null as tot_vol_sgl_ca,
            null as tot_vol_sgl_ul,
            null as tot_vol_sp_base_uom,
            null as tot_vol_sp_base_uom_mgmt_adjustment,
            null as tot_vol_sp_base_uom_pre_adjustment,
            null as tot_vol_sp_kg_uom,
            null as tot_vol_sp_kg_uom_mgmt_adjustment,
            null as tot_vol_sp_kg_uom_pre_adjustment,
            null as tot_vol_sp_ul_uom,
            null as tot_vol_sp_ul_uom_mgmt_adjustment,
            null as tot_vol_sp_ul_uom_pre_adjustment,
            old_budget.budget_qty_ul as tot_vol_ul,
            old_budget.budget_qty_kg as fcf_tot_vol_kg,
            old_budget.budget_qty_ca as fcf_tot_vol_ca,
            old_budget.budget_qty_ca as fcf_tot_vol_ul,
            null as fcf_base_vol_kg,
            null as fcf_base_vol_ca,
            null as fcf_base_vol_ul,
            null as fcf_promo_vol_kg,
            null as fcf_promo_vol_ca,
            null as fcf_promo_vol_ul,
            null as fcf_over_vol_kg,
            null as fcf_over_vol_ca,
            null as fcf_over_vol_ul,
            null as gl_unit_price,
            null as raw_material_unit_price,
            old_budget.rawmats_cost_amt
            * {{ sign_flip }} as ap_tot_prime_cost_standard_raw,
            null as packaging_unit_price,
            old_budget.pack_cost_amt
            * {{ sign_flip }} as ap_tot_prime_cost_standard_packaging,
            null as labour_unit_price,
            old_budget.labour_cost_amt
            * {{ sign_flip }} as ap_tot_prime_cost_standard_labour,
            null as bought_in_unit_price,
            old_budget.boughtin_cost_amt
            * {{ sign_flip }} as ap_tot_prime_cost_standard_bought_in,
            null as other_unit_price,
            old_budget.rye_adj_cost_amt
            * {{ sign_flip }} as ap_tot_prime_cost_standard_other,
            null as co_pack_unit_price,
            old_budget.copack_cost_amt
            * {{ sign_flip }} as ap_tot_prime_cost_standard_co_pack
        from fact old_budget
        join
            pass4_calcs calcs
            on nvl(old_budget.calendar_date, '01-JAN-2000')
            = nvl(calcs.calendar_date, '01-JAN-2000')
            and nvl(to_char(old_budget.item_guid), 'X')
            = nvl(to_char(calcs.item_guid), 'X')
            and nvl(upper(trim(old_budget.trade_type_code)), 'X')
            = nvl(upper(trim(calcs.plan_source_customer_code)), 'X')
            and nvl(upper(trim(old_budget.frozen_forecast)), 'X')
            = nvl(upper(trim(calcs.frozen_forecast)), 'X')
            and nvl(upper(trim(old_budget.source_item_identifier)), 'X')
            = nvl(upper(trim(calcs.source_item_identifier)), 'X')
        left join
            item_ext
            on upper(trim(item_ext.source_item_identifier))
            = upper(trim(old_budget.source_item_identifier))
    )

select
    cast(calendar_date as timestamp_ntz(9)) as calendar_date,
    cast(substring(is_vol_total_nonzero, 1, 20) as text(20)) as is_vol_total_nonzero,
    cast(substring(isonpromo_si, 1, 20) as text(20)) as isonpromo_si,
    cast(substring(isonpromo_so, 1, 20) as text(20)) as isonpromo_so,
    cast(substring(ispreorpostpromo_si, 1, 20) as text(20)) as ispreorpostpromo_si,
    cast(substring(ispreorpostpromo_so, 1, 20) as text(20)) as ispreorpostpromo_so,
    cast(item_guid as text(255)) as item_guid,
    cast(substring(listingactive, 1, 20) as text(20)) as listingactive,
    cast(plan_customer_addr_number_guid as text(255)) as plan_customer_addr_number_guid,
    cast(
        substring(plan_source_customer_code, 1, 255) as text(255)
    ) as plan_source_customer_code,
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
    cast(substring(scen_code, 1, 255) as text(255)) as scen_code,
    cast(scenario_guid as text(255)) as scenario_guid,
    cast(substring(frozen_forecast, 1, 255) as text(255)) as frozen_forecast,
    cast(snapshot_date as date) as snapshot_date,
    cast(
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
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
    cast(
        {{
            dbt_utils.surrogate_key(
                [
                    "cast(ltrim(rtrim(upper(substring(source_system,1,255)))) as text(255))",
                    "cast(ltrim(rtrim(substring(source_item_identifier,1,255))) as text(255))",
                    "cast(ltrim(rtrim(substring(frozen_forecast,1,255))) as text(255))",
                    "cast(ltrim(rtrim(substring(plan_source_customer_code,1,255))) as text(255))",
                    "cast(calendar_date as timestamp_ntz(9))",
                ]
            )
        }}
        as text(255)
    ) as unique_key
from fct_wbx_sls_budget
