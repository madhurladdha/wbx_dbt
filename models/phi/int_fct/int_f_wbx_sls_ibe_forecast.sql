{{
    config(
        tags=["sales", "ibe_forecast","adhoc","ibe"],
    )
}}
{% set sign_flip = 1 %}
with
fact as (
    select *
    from {{ ref("int_f_wbx_sls_ibe_forecast_fin") }}
),

item_ext as (
    select
        source_system,
        source_item_identifier,
        max(dummy_product_flag) as dummy_product_flag
    from {{ ref("dim_wbx_item_ext") }}
    group by source_system, source_item_identifier
),

ref_effective_currency_dim as (
    select distinct
        source_system as source_system,
        company_code as company_code,
        company_default_currency_code as company_default_currency_code,
        parent_currency_code as parent_currency_code,
        effective_date as effective_date,
        expiration_date as expiration_date
    from {{ ref("src_ref_effective_currency_dim") }}
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
        )
        + (old_budget.fixed_annual_payment * {{ sign_flip }})
            as ap_fixed_trade_cust_invoiced,
        (mif_customer_mktg_amt * {{ sign_flip }})
        + (mif_category_amt * {{ sign_flip }})
        + 0
        + (mif_field_mktg_amt * {{ sign_flip }})
        + (
            mif_range_support_incent_amt * {{ sign_flip }}
        ) as ap_fixed_trade_non_cust_invoiced,
        old_budget.gross_value_amt
        + (old_budget.pif_trade_avp_amt * {{ sign_flip }})
            as ap_gross_sales_value,
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
        + (old_budget.pack_cost_amt * {{ sign_flip }})
            as intermediate_total_pcos,
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
        + (old_budget.pif_trade_avp_amt * {{ sign_flip }})
            as ap_variable_trade
    from fact as old_budget
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
        calcs.ap_fixed_trade_non_cust_invoiced
            as ap_fixed_trade_non_cust_invoiced,
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
    from fact as old_budget
    inner join
        pass1_calcs as calcs
        on
            nvl(old_budget.calendar_date, '01-JAN-2000')
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
        calcs.ap_fixed_trade_non_cust_invoiced
            as ap_fixed_trade_non_cust_invoiced,
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
    from fact as old_budget
    inner join
        pass2_calcs as calcs
        on
            nvl(old_budget.calendar_date, '01-JAN-2000')
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
        'EUR' as txn_currency,
        old_budget.trade_type_code as plan_source_customer_code,
        old_budget.frozen_forecast as frozen_forecast,
        old_budget.source_item_identifier as source_item_identifier,
        -- PASS 1 CALCS
        calcs.ap_fixed_trade_cust_invoiced as ap_fixed_trade_cust_invoiced,
        calcs.ap_fixed_trade_non_cust_invoiced
            as ap_fixed_trade_non_cust_invoiced,
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
    from fact as old_budget
    inner join
        pass3_calcs as calcs
        on
            nvl(old_budget.calendar_date, '01-JAN-2000')
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
        old_budget.snapshot_date as snapshot_date,
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
        old_budget.settlement_amt
        * {{ sign_flip }} as ap_early_settlement_disc,
        null as ap_early_settlement_disc_mgmt_adjustment,
        null as ap_early_settlement_disc_pre_adjustment,
        old_budget.edlp_amt * {{ sign_flip }} as ap_everyday_low_prices,
        null as ap_everyday_low_prices_mgmt_adjustment,
        null as ap_everyday_low_prices_pre_adjustment,
        old_budget.mif_field_mktg_amt
        * {{ sign_flip }} as ap_field_marketing,
        null as ap_field_marketing_mgmt_adjustment,
        null as ap_field_marketing_pre_adjustment,
        old_budget.mif_isa_amt
        * {{ sign_flip }} as ap_fixed_annual_payments,
        null as ap_fixed_annual_payments_mgmt_adjustment,
        null as ap_fixed_annual_payments_pre_adjustment,
        calcs.ap_fixed_trade_cust_invoiced as ap_fixed_trade_cust_invoiced,
        calcs.ap_fixed_trade_non_cust_invoiced
            as ap_fixed_trade_non_cust_invoiced,
        null as ap_gcat_actuals,
        null as ap_gcat_standard,
        calcs.ap_gross_margin_actual as ap_gross_margin_actual,
        calcs.ap_gross_margin_standard as ap_gross_margin_standard,
        calcs.ap_gross_sales_value as ap_gross_sales_value,
        old_budget.gross_value_amt as ap_gross_selling_value,
        null as ap_gross_selling_value_mgmt_adjustment,
        null as ap_gross_selling_value_pre_adjustment,
        old_budget.back_margin_amt
        * {{ sign_flip }} as ap_growth_incentives,
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
        old_budget.pif_trade_enh_amt
        * {{ sign_flip }} as ap_other_direct_payments,
        null as ap_other_direct_payments_mgmt_adjustment,
        null as ap_other_direct_payments_pre_adjustment,
        null as ap_other_indirect_payments,
        null as ap_other_indirect_payments_mgmt_adjustment,
        null as ap_other_indirect_payments_pre_adjustment,
        old_budget.rsa_amt * {{ sign_flip }} as ap_permanent_disc,
        old_budget.pif_isa_amt * {{ sign_flip }} as ap_promo_fixed_funding,
        null as ap_promo_fixed_funding_mgmt_adjustment,
        null as ap_promo_fixed_funding_pre_adjustment,
        rsatotal_new as ap_range_support_allowance,
        null as ap_range_support_allowance_mgmt_adjustment,
        null as ap_range_support_allowance_pre_adjustment,
        old_budget.gincent_amt
        * {{ sign_flip }} as ap_range_support_incentives,
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
        * {{ sign_flip }} as ap_retro,
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
        * {{ sign_flip }} as ap_tot_prime_cost_standard_co_pack,
        old_budget.company_code as company_code,
        calcs.txn_currency,
        ref_effective_currency_dim.company_default_currency_code
            as base_currency,
        ref_effective_currency_dim.parent_currency_code as pcomp_currency,
        'USD' as phi_currency,
        case
            when
                ref_effective_currency_dim.company_default_currency_code
                = txn_currency
                then 1
            else coalesce(txn_conv_rt_lkp.curr_conv_rt, 0)
        end as trans_conv_rt,
        '1' as base_conv_rt,
        case
            when
                ref_effective_currency_dim.company_default_currency_code = 'USD'
                then 1
            else coalesce(phi_conv_rt_lkp.curr_conv_rt, 0)
        end as phi_conv_rt,
        case
            when
                ref_effective_currency_dim.company_default_currency_code
                = ref_effective_currency_dim.parent_currency_code
                then 1
            else coalesce(pcomp_conv_rt_lkp.curr_conv_rt, 0)
        end as pcomp_conv_rt,
        old_budget.consumer_marketing
        * {{ sign_flip }} as ap_tot_consumer_marketing
    from fact as old_budget
    inner join
        pass4_calcs as calcs
        on
            nvl(old_budget.calendar_date, '01-JAN-2000')
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
        on
            upper(trim(item_ext.source_item_identifier))
            = upper(trim(old_budget.source_item_identifier))
    /*-20th Mar 2024 Adding currency conversion logic for ibe forcast  */
    left join
        ref_effective_currency_dim
        on
            ref_effective_currency_dim.source_system = old_budget.source_system
            and ref_effective_currency_dim.company_code
            = old_budget.company_code
            and ref_effective_currency_dim.effective_date
            <= old_budget.calendar_date
            and ref_effective_currency_dim.expiration_date
            >= old_budget.calendar_date
    left join
            {{
                lkp_exchange_rate_daily_oc(
                    "old_budget.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "txn_currency",
                    "old_budget.calendar_date",
                    "txn_conv_rt_LKP",
                )
            }}
    /*currency conv table haven't been roll forwarding the conv rt since 2023*/
    /*using  sub sql instead of macro  to pick the latest conv rt from table */
    left join

        (select
            source_system,
            curr_from_code,
            curr_to_code,
            eff_from_d,
            curr_conv_rt,
            row_number()
                over (
                    partition by source_system, curr_from_code, curr_to_code
                    order by curr_from_code, curr_to_code, eff_from_d desc
                )
                as row_num
        from {{ ref("v_dim_exchange_rate_dly_oc") }}) as phi_conv_rt_lkp
        on
            phi_conv_rt_lkp.source_system = old_budget.source_system
            and phi_conv_rt_lkp.curr_from_code
            = ref_effective_currency_dim.company_default_currency_code
            and phi_conv_rt_lkp.curr_to_code = 'USD'
            and phi_conv_rt_lkp.row_num = 1

    left join
            {{
                lkp_exchange_rate_daily_oc(
                    "old_budget.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "ref_effective_currency_dim.parent_currency_code",
                    "old_budget.calendar_date",
                    "pcomp_conv_rt_LKP",
                )
            }}

),

curr_conv as (
    select
        calendar_date,
        is_vol_total_nonzero,
        company_code,
        base_currency,
        txn_currency,
        phi_currency,
        pcomp_currency,
        base_conv_rt,
        trans_conv_rt,
        phi_conv_rt,
        pcomp_conv_rt,
        isonpromo_si,
        isonpromo_so,
        ispreorpostpromo_si,
        ispreorpostpromo_so,
        item_guid,
        listingactive,
        plan_customer_addr_number_guid,
        plan_source_customer_code,
        qty_ca_cannib_loss_si,
        qty_ca_cannib_loss_so,
        qty_ca_effective_base_fc_si,
        qty_ca_effective_base_fc_so,
        qty_ca_override_si,
        qty_ca_override_so,
        qty_ca_pp_dip_si,
        qty_ca_pp_dip_so,
        qty_ca_promo_total_si,
        qty_ca_promo_total_so,
        qty_ca_si_actual,
        qty_ca_so_actual,
        qty_ca_stat_base_fc_si,
        qty_ca_stat_base_fc_so,
        qty_ca_total_adjust_si,
        qty_ca_total_adjust_so,
        qty_ca_total_si,
        qty_ca_total_so,
        qty_kg_cannib_loss_si,
        qty_kg_cannib_loss_so,
        qty_kg_effective_base_fc_si,
        qty_kg_effective_base_fc_so,
        qty_kg_override_si,
        qty_kg_override_so,
        qty_kg_pp_dip_si,
        qty_kg_pp_dip_so,
        qty_kg_promo_total_si,
        qty_kg_promo_total_so,
        qty_kg_si_actual,
        qty_kg_so_actual,
        qty_kg_stat_base_fc_si,
        qty_kg_stat_base_fc_so,
        qty_kg_total_adjust_si,
        qty_kg_total_adjust_so,
        qty_kg_total_si,
        qty_kg_total_so,
        qty_ul_cannib_loss_si,
        qty_ul_cannib_loss_so,
        qty_ul_effective_base_fc_si,
        qty_ul_effective_base_fc_so,
        qty_ul_override_si,
        qty_ul_override_so,
        qty_ul_pp_dip_si,
        qty_ul_pp_dip_so,
        qty_ul_promo_total_si,
        qty_ul_promo_total_so,
        qty_ul_si_actual,
        qty_ul_so_actual,
        qty_ul_stat_base_fc_si,
        qty_ul_stat_base_fc_so,
        qty_ul_total_adjust_si,
        qty_ul_total_adjust_so,
        qty_ul_total_si,
        qty_ul_total_so,
        scen_code,
        scenario_guid,
        frozen_forecast,
        snapshot_date,
        source_item_identifier,
        source_system,
        total_baseretentionpercentage,
        total_si_preorpostdippercentage,
        total_so_preorpostdippercentage,
        ap_added_value_pack as ap_added_value_pack,
        trans_conv_rt * ap_added_value_pack as trans_ap_added_value_pack,
        phi_conv_rt * ap_added_value_pack as phi_ap_added_value_pack,
        pcomp_conv_rt * ap_added_value_pack as pcomp_ap_added_value_pack,
        ap_avp_disc as ap_avp_disc,
        trans_conv_rt * ap_avp_disc as trans_ap_avp_disc,
        phi_conv_rt * ap_avp_disc as phi_ap_avp_disc,
        pcomp_conv_rt * ap_avp_disc as pcomp_ap_avp_disc,
        ap_avp_disc_mgmt_adjustment as ap_avp_disc_mgmt_adjustment,
        trans_conv_rt
        * ap_avp_disc_mgmt_adjustment as trans_ap_avp_disc_mgmt_adjustment,
        phi_conv_rt
        * ap_avp_disc_mgmt_adjustment as phi_ap_avp_disc_mgmt_adjustment,
        pcomp_conv_rt
        * ap_avp_disc_mgmt_adjustment as pcomp_ap_avp_disc_mgmt_adjustment,
        ap_avp_disc_pre_adjustment as ap_avp_disc_pre_adjustment,
        trans_conv_rt
        * ap_avp_disc_pre_adjustment as trans_ap_avp_disc_pre_adjustment,
        phi_conv_rt
        * ap_avp_disc_pre_adjustment as phi_ap_avp_disc_pre_adjustment,
        pcomp_conv_rt
        * ap_avp_disc_pre_adjustment as pcomp_ap_avp_disc_pre_adjustment,
        ap_cash_disc as ap_cash_disc,
        trans_conv_rt * ap_cash_disc as trans_ap_cash_disc,
        phi_conv_rt * ap_cash_disc as phi_ap_cash_disc,
        pcomp_conv_rt * ap_cash_disc as pcomp_ap_cash_disc,
        ap_cash_disc_mgmt_adjustment as ap_cash_disc_mgmt_adjustment,
        trans_conv_rt
        * ap_cash_disc_mgmt_adjustment as trans_ap_cash_disc_mgmt_adjustment,
        phi_conv_rt
        * ap_cash_disc_mgmt_adjustment as phi_ap_cash_disc_mgmt_adjustment,
        pcomp_conv_rt
        * ap_cash_disc_mgmt_adjustment as pcomp_ap_cash_disc_mgmt_adjustment,
        ap_cash_disc_pre_adjustment as ap_cash_disc_pre_adjustment,
        trans_conv_rt
        * ap_cash_disc_pre_adjustment as trans_ap_cash_disc_pre_adjustment,
        phi_conv_rt
        * ap_cash_disc_pre_adjustment as phi_ap_cash_disc_pre_adjustment,
        pcomp_conv_rt
        * ap_cash_disc_pre_adjustment as pcomp_ap_cash_disc_pre_adjustment,
        ap_category as ap_category,
        trans_conv_rt * ap_category as trans_ap_category,
        phi_conv_rt * ap_category as phi_ap_category,
        pcomp_conv_rt * ap_category as pcomp_ap_category,
        ap_category_mgmt_adjustment as ap_category_mgmt_adjustment,
        trans_conv_rt
        * ap_category_mgmt_adjustment as trans_ap_category_mgmt_adjustment,
        phi_conv_rt
        * ap_category_mgmt_adjustment as phi_ap_category_mgmt_adjustment,
        pcomp_conv_rt
        * ap_category_mgmt_adjustment as pcomp_ap_category_mgmt_adjustment,
        ap_category_pre_adjustment as ap_category_pre_adjustment,
        trans_conv_rt
        * ap_category_pre_adjustment as trans_ap_category_pre_adjustment,
        phi_conv_rt
        * ap_category_pre_adjustment as phi_ap_category_pre_adjustment,
        pcomp_conv_rt
        * ap_category_pre_adjustment as pcomp_ap_category_pre_adjustment,
        ap_direct_shopper_marketing as ap_direct_shopper_marketing,
        trans_conv_rt
        * ap_direct_shopper_marketing as trans_ap_direct_shopper_marketing,
        phi_conv_rt
        * ap_direct_shopper_marketing as phi_ap_direct_shopper_marketing,
        pcomp_conv_rt
        * ap_direct_shopper_marketing as pcomp_ap_direct_shopper_marketing,
        ap_direct_shopper_marketing_mgmt_adjustment
            as ap_direct_shopper_marketing_mgmt_adjustment,
        trans_conv_rt
        * ap_direct_shopper_marketing_mgmt_adjustment
            as trans_ap_direct_shopper_marketing_mgmt_adjustment,
        phi_conv_rt
        * ap_direct_shopper_marketing_mgmt_adjustment
            as phi_ap_direct_shopper_marketing_mgmt_adjustment,
        pcomp_conv_rt
        * ap_direct_shopper_marketing_mgmt_adjustment
            as pcomp_ap_direct_shopper_marketing_mgmt_adjustment,
        ap_direct_shopper_marketing_pre_adjustment
            as ap_direct_shopper_marketing_pre_adjustment,
        trans_conv_rt
        * ap_direct_shopper_marketing_pre_adjustment
            as trans_ap_direct_shopper_marketing_pre_adjustment,
        phi_conv_rt
        * ap_direct_shopper_marketing_pre_adjustment
            as phi_ap_direct_shopper_marketing_pre_adjustment,
        pcomp_conv_rt
        * ap_direct_shopper_marketing_pre_adjustment
            as pcomp_ap_direct_shopper_marketing_pre_adjustment,
        ap_early_settlement_disc as ap_early_settlement_disc,
        trans_conv_rt
        * ap_early_settlement_disc as trans_ap_early_settlement_disc,
        phi_conv_rt * ap_early_settlement_disc as phi_ap_early_settlement_disc,
        pcomp_conv_rt
        * ap_early_settlement_disc as pcomp_ap_early_settlement_disc,
        ap_early_settlement_disc_mgmt_adjustment
            as ap_early_settlement_disc_mgmt_adjustment,
        trans_conv_rt
        * ap_early_settlement_disc_mgmt_adjustment
            as trans_ap_early_settlement_disc_mgmt_adjustment,
        phi_conv_rt
        * ap_early_settlement_disc_mgmt_adjustment
            as phi_ap_early_settlement_disc_mgmt_adjustment,
        pcomp_conv_rt
        * ap_early_settlement_disc_mgmt_adjustment
            as pcomp_ap_early_settlement_disc_mgmt_adjustment,
        ap_early_settlement_disc_pre_adjustment
            as ap_early_settlement_disc_pre_adjustment,
        trans_conv_rt
        * ap_early_settlement_disc_pre_adjustment
            as trans_ap_early_settlement_disc_pre_adjustment,
        phi_conv_rt
        * ap_early_settlement_disc_pre_adjustment
            as phi_ap_early_settlement_disc_pre_adjustment,
        pcomp_conv_rt
        * ap_early_settlement_disc_pre_adjustment
            as pcomp_ap_early_settlement_disc_pre_adjustment,
        ap_everyday_low_prices as ap_everyday_low_prices,
        trans_conv_rt * ap_everyday_low_prices as trans_ap_everyday_low_prices,
        phi_conv_rt * ap_everyday_low_prices as phi_ap_everyday_low_prices,
        pcomp_conv_rt * ap_everyday_low_prices as pcomp_ap_everyday_low_prices,
        ap_everyday_low_prices_mgmt_adjustment
            as ap_everyday_low_prices_mgmt_adjustment,
        trans_conv_rt
        * ap_everyday_low_prices_mgmt_adjustment
            as trans_ap_everyday_low_prices_mgmt_adjustment,
        phi_conv_rt
        * ap_everyday_low_prices_mgmt_adjustment
            as phi_ap_everyday_low_prices_mgmt_adjustment,
        pcomp_conv_rt
        * ap_everyday_low_prices_mgmt_adjustment
            as pcomp_ap_everyday_low_prices_mgmt_adjustment,
        ap_everyday_low_prices_pre_adjustment
            as ap_everyday_low_prices_pre_adjustment,
        trans_conv_rt
        * ap_everyday_low_prices_pre_adjustment
            as trans_ap_everyday_low_prices_pre_adjustment,
        phi_conv_rt
        * ap_everyday_low_prices_pre_adjustment
            as phi_ap_everyday_low_prices_pre_adjustment,
        pcomp_conv_rt
        * ap_everyday_low_prices_pre_adjustment
            as pcomp_ap_everyday_low_prices_pre_adjustment,
        ap_field_marketing as ap_field_marketing,
        trans_conv_rt * ap_field_marketing as trans_ap_field_marketing,
        phi_conv_rt * ap_field_marketing as phi_ap_field_marketing,
        pcomp_conv_rt * ap_field_marketing as pcomp_ap_field_marketing,
        ap_field_marketing_mgmt_adjustment
            as ap_field_marketing_mgmt_adjustment,
        trans_conv_rt
        * ap_field_marketing_mgmt_adjustment
            as trans_ap_field_marketing_mgmt_adjustment,
        phi_conv_rt
        * ap_field_marketing_mgmt_adjustment
            as phi_ap_field_marketing_mgmt_adjustment,
        pcomp_conv_rt
        * ap_field_marketing_mgmt_adjustment
            as pcomp_ap_field_marketing_mgmt_adjustment,
        ap_field_marketing_pre_adjustment as ap_field_marketing_pre_adjustment,
        trans_conv_rt
        * ap_field_marketing_pre_adjustment
            as trans_ap_field_marketing_pre_adjustment,
        phi_conv_rt
        * ap_field_marketing_pre_adjustment
            as phi_ap_field_marketing_pre_adjustment,
        pcomp_conv_rt
        * ap_field_marketing_pre_adjustment
            as pcomp_ap_field_marketing_pre_adjustment,
        ap_fixed_annual_payments as ap_fixed_annual_payments,
        trans_conv_rt
        * ap_fixed_annual_payments as trans_ap_fixed_annual_payments,
        phi_conv_rt * ap_fixed_annual_payments as phi_ap_fixed_annual_payments,
        pcomp_conv_rt
        * ap_fixed_annual_payments as pcomp_ap_fixed_annual_payments,
        ap_fixed_annual_payments_mgmt_adjustment
            as ap_fixed_annual_payments_mgmt_adjustment,
        trans_conv_rt
        * ap_fixed_annual_payments_mgmt_adjustment
            as trans_ap_fixed_annual_payments_mgmt_adjustment,
        phi_conv_rt
        * ap_fixed_annual_payments_mgmt_adjustment
            as phi_ap_fixed_annual_payments_mgmt_adjustment,
        pcomp_conv_rt
        * ap_fixed_annual_payments_mgmt_adjustment
            as pcomp_ap_fixed_annual_payments_mgmt_adjustment,
        ap_fixed_annual_payments_pre_adjustment
            as ap_fixed_annual_payments_pre_adjustment,
        trans_conv_rt
        * ap_fixed_annual_payments_pre_adjustment
            as trans_ap_fixed_annual_payments_pre_adjustment,
        phi_conv_rt
        * ap_fixed_annual_payments_pre_adjustment
            as phi_ap_fixed_annual_payments_pre_adjustment,
        pcomp_conv_rt
        * ap_fixed_annual_payments_pre_adjustment
            as pcomp_ap_fixed_annual_payments_pre_adjustment,
        ap_fixed_trade_cust_invoiced as ap_fixed_trade_cust_invoiced,
        trans_conv_rt
        * ap_fixed_trade_cust_invoiced as trans_ap_fixed_trade_cust_invoiced,
        phi_conv_rt
        * ap_fixed_trade_cust_invoiced as phi_ap_fixed_trade_cust_invoiced,
        pcomp_conv_rt
        * ap_fixed_trade_cust_invoiced as pcomp_ap_fixed_trade_cust_invoiced,
        ap_fixed_trade_non_cust_invoiced as ap_fixed_trade_non_cust_invoiced,
        trans_conv_rt
        * ap_fixed_trade_non_cust_invoiced
            as trans_ap_fixed_trade_non_cust_invoiced,
        phi_conv_rt
        * ap_fixed_trade_non_cust_invoiced
            as phi_ap_fixed_trade_non_cust_invoiced,
        pcomp_conv_rt
        * ap_fixed_trade_non_cust_invoiced
            as pcomp_ap_fixed_trade_non_cust_invoiced,
        ap_gcat_actuals as ap_gcat_actuals,
        trans_conv_rt * ap_gcat_actuals as trans_ap_gcat_actuals,
        phi_conv_rt * ap_gcat_actuals as phi_ap_gcat_actuals,
        pcomp_conv_rt * ap_gcat_actuals as pcomp_ap_gcat_actuals,
        ap_gcat_standard as ap_gcat_standard,
        trans_conv_rt * ap_gcat_standard as trans_ap_gcat_standard,
        phi_conv_rt * ap_gcat_standard as phi_ap_gcat_standard,
        pcomp_conv_rt * ap_gcat_standard as pcomp_ap_gcat_standard,
        ap_gross_margin_actual as ap_gross_margin_actual,
        trans_conv_rt * ap_gross_margin_actual as trans_ap_gross_margin_actual,
        phi_conv_rt * ap_gross_margin_actual as phi_ap_gross_margin_actual,
        pcomp_conv_rt * ap_gross_margin_actual as pcomp_ap_gross_margin_actual,
        ap_gross_margin_standard as ap_gross_margin_standard,
        trans_conv_rt
        * ap_gross_margin_standard as trans_ap_gross_margin_standard,
        phi_conv_rt * ap_gross_margin_standard as phi_ap_gross_margin_standard,
        pcomp_conv_rt
        * ap_gross_margin_standard as pcomp_ap_gross_margin_standard,
        ap_gross_sales_value as ap_gross_sales_value,
        trans_conv_rt * ap_gross_sales_value as trans_ap_gross_sales_value,
        phi_conv_rt * ap_gross_sales_value as phi_ap_gross_sales_value,
        pcomp_conv_rt * ap_gross_sales_value as pcomp_ap_gross_sales_value,
        ap_gross_selling_value as ap_gross_selling_value,
        trans_conv_rt * ap_gross_selling_value as trans_ap_gross_selling_value,
        phi_conv_rt * ap_gross_selling_value as phi_ap_gross_selling_value,
        pcomp_conv_rt * ap_gross_selling_value as pcomp_ap_gross_selling_value,
        ap_gross_selling_value_mgmt_adjustment
            as ap_gross_selling_value_mgmt_adjustment,
        trans_conv_rt
        * ap_gross_selling_value_mgmt_adjustment
            as trans_ap_gross_selling_value_mgmt_adjustment,
        phi_conv_rt
        * ap_gross_selling_value_mgmt_adjustment
            as phi_ap_gross_selling_value_mgmt_adjustment,
        pcomp_conv_rt
        * ap_gross_selling_value_mgmt_adjustment
            as pcomp_ap_gross_selling_value_mgmt_adjustment,
        ap_gross_selling_value_pre_adjustment
            as ap_gross_selling_value_pre_adjustment,
        trans_conv_rt
        * ap_gross_selling_value_pre_adjustment
            as trans_ap_gross_selling_value_pre_adjustment,
        phi_conv_rt
        * ap_gross_selling_value_pre_adjustment
            as phi_ap_gross_selling_value_pre_adjustment,
        pcomp_conv_rt
        * ap_gross_selling_value_pre_adjustment
            as pcomp_ap_gross_selling_value_pre_adjustment,
        ap_growth_incentives as ap_growth_incentives,
        trans_conv_rt * ap_growth_incentives as trans_ap_growth_incentives,
        phi_conv_rt * ap_growth_incentives as phi_ap_growth_incentives,
        pcomp_conv_rt * ap_growth_incentives as pcomp_ap_growth_incentives,
        ap_growth_incentives_mgmt_adjustment
            as ap_growth_incentives_mgmt_adjustment,
        trans_conv_rt
        * ap_growth_incentives_mgmt_adjustment
            as trans_ap_growth_incentives_mgmt_adjustment,
        phi_conv_rt
        * ap_growth_incentives_mgmt_adjustment
            as phi_ap_growth_incentives_mgmt_adjustment,
        pcomp_conv_rt
        * ap_growth_incentives_mgmt_adjustment
            as pcomp_ap_growth_incentives_mgmt_adjustment,
        ap_growth_incentives_pre_adjustment
            as ap_growth_incentives_pre_adjustment,
        trans_conv_rt
        * ap_growth_incentives_pre_adjustment
            as trans_ap_growth_incentives_pre_adjustment,
        phi_conv_rt
        * ap_growth_incentives_pre_adjustment
            as phi_ap_growth_incentives_pre_adjustment,
        pcomp_conv_rt
        * ap_growth_incentives_pre_adjustment
            as pcomp_ap_growth_incentives_pre_adjustment,
        ap_indirect_shopper_marketing as ap_indirect_shopper_marketing,
        trans_conv_rt
        * ap_indirect_shopper_marketing as trans_ap_indirect_shopper_marketing,
        phi_conv_rt
        * ap_indirect_shopper_marketing as phi_ap_indirect_shopper_marketing,
        pcomp_conv_rt
        * ap_indirect_shopper_marketing as pcomp_ap_indirect_shopper_marketing,
        ap_indirect_shopper_marketing_mgmt_adjustment
            as ap_indirect_shopper_marketing_mgmt_adjustment,
        trans_conv_rt
        * ap_indirect_shopper_marketing_mgmt_adjustment
            as trans_ap_indirect_shopper_marketing_mgmt_adjustment,
        phi_conv_rt
        * ap_indirect_shopper_marketing_mgmt_adjustment
            as phi_ap_indirect_shopper_marketing_mgmt_adjustment,
        pcomp_conv_rt
        * ap_indirect_shopper_marketing_mgmt_adjustment
            as pcomp_ap_indirect_shopper_marketing_mgmt_adjustment,
        ap_indirect_shopper_marketing_pre_adjustment
            as ap_indirect_shopper_marketing_pre_adjustment,
        trans_conv_rt
        * ap_indirect_shopper_marketing_pre_adjustment
            as trans_ap_indirect_shopper_marketing_pre_adjustment,
        phi_conv_rt
        * ap_indirect_shopper_marketing_pre_adjustment
            as phi_ap_indirect_shopper_marketing_pre_adjustment,
        pcomp_conv_rt
        * ap_indirect_shopper_marketing_pre_adjustment
            as pcomp_ap_indirect_shopper_marketing_pre_adjustment,
        ap_invoiced_sales_value as ap_invoiced_sales_value,
        trans_conv_rt
        * ap_invoiced_sales_value as trans_ap_invoiced_sales_value,
        phi_conv_rt * ap_invoiced_sales_value as phi_ap_invoiced_sales_value,
        pcomp_conv_rt
        * ap_invoiced_sales_value as pcomp_ap_invoiced_sales_value,
        ap_net_net_sales_value as ap_net_net_sales_value,
        trans_conv_rt * ap_net_net_sales_value as trans_ap_net_net_sales_value,
        phi_conv_rt * ap_net_net_sales_value as phi_ap_net_net_sales_value,
        pcomp_conv_rt * ap_net_net_sales_value as pcomp_ap_net_net_sales_value,
        ap_net_realisable_revenue as ap_net_realisable_revenue,
        trans_conv_rt
        * ap_net_realisable_revenue as trans_ap_net_realisable_revenue,
        phi_conv_rt
        * ap_net_realisable_revenue as phi_ap_net_realisable_revenue,
        pcomp_conv_rt
        * ap_net_realisable_revenue as pcomp_ap_net_realisable_revenue,
        ap_net_sales_value as ap_net_sales_value,
        trans_conv_rt * ap_net_sales_value as trans_ap_net_sales_value,
        phi_conv_rt * ap_net_sales_value as phi_ap_net_sales_value,
        pcomp_conv_rt * ap_net_sales_value as pcomp_ap_net_sales_value,
        ap_off_invoice_disc as ap_off_invoice_disc,
        trans_conv_rt * ap_off_invoice_disc as trans_ap_off_invoice_disc,
        phi_conv_rt * ap_off_invoice_disc as phi_ap_off_invoice_disc,
        pcomp_conv_rt * ap_off_invoice_disc as pcomp_ap_off_invoice_disc,
        ap_off_invoice_disc_mgmt_adjustment
            as ap_off_invoice_disc_mgmt_adjustment,
        trans_conv_rt
        * ap_off_invoice_disc_mgmt_adjustment
            as trans_ap_off_invoice_disc_mgmt_adjustment,
        phi_conv_rt
        * ap_off_invoice_disc_mgmt_adjustment
            as phi_ap_off_invoice_disc_mgmt_adjustment,
        pcomp_conv_rt
        * ap_off_invoice_disc_mgmt_adjustment
            as pcomp_ap_off_invoice_disc_mgmt_adjustment,
        ap_off_invoice_disc_pre_adjustment
            as ap_off_invoice_disc_pre_adjustment,
        trans_conv_rt
        * ap_off_invoice_disc_pre_adjustment
            as trans_ap_off_invoice_disc_pre_adjustment,
        phi_conv_rt
        * ap_off_invoice_disc_pre_adjustment
            as phi_ap_off_invoice_disc_pre_adjustment,
        pcomp_conv_rt
        * ap_off_invoice_disc_pre_adjustment
            as pcomp_ap_off_invoice_disc_pre_adjustment,
        ap_other_direct_payments as ap_other_direct_payments,
        trans_conv_rt
        * ap_other_direct_payments as trans_ap_other_direct_payments,
        phi_conv_rt * ap_other_direct_payments as phi_ap_other_direct_payments,
        pcomp_conv_rt
        * ap_other_direct_payments as pcomp_ap_other_direct_payments,
        ap_other_direct_payments_mgmt_adjustment
            as ap_other_direct_payments_mgmt_adjustment,
        trans_conv_rt
        * ap_other_direct_payments_mgmt_adjustment
            as trans_ap_other_direct_payments_mgmt_adjustment,
        phi_conv_rt
        * ap_other_direct_payments_mgmt_adjustment
            as phi_ap_other_direct_payments_mgmt_adjustment,
        pcomp_conv_rt
        * ap_other_direct_payments_mgmt_adjustment
            as pcomp_ap_other_direct_payments_mgmt_adjustment,
        ap_other_direct_payments_pre_adjustment
            as ap_other_direct_payments_pre_adjustment,
        trans_conv_rt
        * ap_other_direct_payments_pre_adjustment
            as trans_ap_other_direct_payments_pre_adjustment,
        phi_conv_rt
        * ap_other_direct_payments_pre_adjustment
            as phi_ap_other_direct_payments_pre_adjustment,
        pcomp_conv_rt
        * ap_other_direct_payments_pre_adjustment
            as pcomp_ap_other_direct_payments_pre_adjustment,
        ap_other_indirect_payments as ap_other_indirect_payments,
        trans_conv_rt
        * ap_other_indirect_payments as trans_ap_other_indirect_payments,
        phi_conv_rt
        * ap_other_indirect_payments as phi_ap_other_indirect_payments,
        pcomp_conv_rt
        * ap_other_indirect_payments as pcomp_ap_other_indirect_payments,
        ap_other_indirect_payments_mgmt_adjustment
            as ap_other_indirect_payments_mgmt_adjustment,
        trans_conv_rt
        * ap_other_indirect_payments_mgmt_adjustment
            as trans_ap_other_indirect_payments_mgmt_adjustment,
        phi_conv_rt
        * ap_other_indirect_payments_mgmt_adjustment
            as phi_ap_other_indirect_payments_mgmt_adjustment,
        pcomp_conv_rt
        * ap_other_indirect_payments_mgmt_adjustment
            as pcomp_ap_other_indirect_payments_mgmt_adjustment,
        ap_other_indirect_payments_pre_adjustment
            as ap_other_indirect_payments_pre_adjustment,
        trans_conv_rt
        * ap_other_indirect_payments_pre_adjustment
            as trans_ap_other_indirect_payments_pre_adjustment,
        phi_conv_rt
        * ap_other_indirect_payments_pre_adjustment
            as phi_ap_other_indirect_payments_pre_adjustment,
        pcomp_conv_rt
        * ap_other_indirect_payments_pre_adjustment
            as pcomp_ap_other_indirect_payments_pre_adjustment,
        ap_permanent_disc as ap_permanent_disc,
        trans_conv_rt * ap_permanent_disc as trans_ap_permanent_disc,
        phi_conv_rt * ap_permanent_disc as phi_ap_permanent_disc,
        pcomp_conv_rt * ap_permanent_disc as pcomp_ap_permanent_disc,
        ap_promo_fixed_funding as ap_promo_fixed_funding,
        trans_conv_rt * ap_promo_fixed_funding as trans_ap_promo_fixed_funding,
        phi_conv_rt * ap_promo_fixed_funding as phi_ap_promo_fixed_funding,
        pcomp_conv_rt * ap_promo_fixed_funding as pcomp_ap_promo_fixed_funding,
        ap_promo_fixed_funding_mgmt_adjustment
            as ap_promo_fixed_funding_mgmt_adjustment,
        trans_conv_rt
        * ap_promo_fixed_funding_mgmt_adjustment
            as trans_ap_promo_fixed_funding_mgmt_adjustment,
        phi_conv_rt
        * ap_promo_fixed_funding_mgmt_adjustment
            as phi_ap_promo_fixed_funding_mgmt_adjustment,
        pcomp_conv_rt
        * ap_promo_fixed_funding_mgmt_adjustment
            as pcomp_ap_promo_fixed_funding_mgmt_adjustment,
        ap_promo_fixed_funding_pre_adjustment
            as ap_promo_fixed_funding_pre_adjustment,
        trans_conv_rt
        * ap_promo_fixed_funding_pre_adjustment
            as trans_ap_promo_fixed_funding_pre_adjustment,
        phi_conv_rt
        * ap_promo_fixed_funding_pre_adjustment
            as phi_ap_promo_fixed_funding_pre_adjustment,
        pcomp_conv_rt
        * ap_promo_fixed_funding_pre_adjustment
            as pcomp_ap_promo_fixed_funding_pre_adjustment,
        ap_range_support_allowance as ap_range_support_allowance,
        ap_range_support_allowance as trans_ap_range_support_allowance,
        ap_range_support_allowance as phi_ap_range_support_allowance,
        ap_range_support_allowance as pcomp_ap_range_support_allowance,
        ap_range_support_allowance_mgmt_adjustment
            as ap_range_support_allowance_mgmt_adjustment,
        trans_conv_rt
        * ap_range_support_allowance_mgmt_adjustment
            as trans_ap_range_support_allowance_mgmt_adjustment,
        phi_conv_rt
        * ap_range_support_allowance_mgmt_adjustment
            as phi_ap_range_support_allowance_mgmt_adjustment,
        pcomp_conv_rt
        * ap_range_support_allowance_mgmt_adjustment
            as pcomp_ap_range_support_allowance_mgmt_adjustment,
        ap_range_support_allowance_pre_adjustment
            as ap_range_support_allowance_pre_adjustment,
        trans_conv_rt
        * ap_range_support_allowance_pre_adjustment
            as trans_ap_range_support_allowance_pre_adjustment,
        phi_conv_rt
        * ap_range_support_allowance_pre_adjustment
            as phi_ap_range_support_allowance_pre_adjustment,
        pcomp_conv_rt
        * ap_range_support_allowance_pre_adjustment
            as pcomp_ap_range_support_allowance_pre_adjustment,
        ap_range_support_incentives as ap_range_support_incentives,
        trans_conv_rt
        * ap_range_support_incentives as trans_ap_range_support_incentives,
        phi_conv_rt
        * ap_range_support_incentives as phi_ap_range_support_incentives,
        pcomp_conv_rt
        * ap_range_support_incentives as pcomp_ap_range_support_incentives,
        ap_range_support_incentives_mgmt_adjustment
            as ap_range_support_incentives_mgmt_adjustment,
        trans_conv_rt
        * ap_range_support_incentives_mgmt_adjustment
            as trans_ap_range_support_incentives_mgmt_adjustment,
        phi_conv_rt
        * ap_range_support_incentives_mgmt_adjustment
            as phi_ap_range_support_incentives_mgmt_adjustment,
        pcomp_conv_rt
        * ap_range_support_incentives_mgmt_adjustment
            as pcomp_ap_range_support_incentives_mgmt_adjustment,
        ap_range_support_incentives_pre_adjustment
            as ap_range_support_incentives_pre_adjustment,
        trans_conv_rt
        * ap_range_support_incentives_pre_adjustment
            as trans_ap_range_support_incentives_pre_adjustment,
        phi_conv_rt
        * ap_range_support_incentives_pre_adjustment
            as phi_ap_range_support_incentives_pre_adjustment,
        pcomp_conv_rt
        * ap_range_support_incentives_pre_adjustment
            as pcomp_ap_range_support_incentives_pre_adjustment,
        ap_retail_cost_of_sales as ap_retail_cost_of_sales,
        trans_conv_rt
        * ap_retail_cost_of_sales as trans_ap_retail_cost_of_sales,
        phi_conv_rt * ap_retail_cost_of_sales as phi_ap_retail_cost_of_sales,
        pcomp_conv_rt
        * ap_retail_cost_of_sales as pcomp_ap_retail_cost_of_sales,
        ap_retail_margin_excl_fixed_funding
            as ap_retail_margin_excl_fixed_funding,
        trans_conv_rt
        * ap_retail_margin_excl_fixed_funding
            as trans_ap_retail_margin_excl_fixed_funding,
        phi_conv_rt
        * ap_retail_margin_excl_fixed_funding
            as phi_ap_retail_margin_excl_fixed_funding,
        pcomp_conv_rt
        * ap_retail_margin_excl_fixed_funding
            as pcomp_ap_retail_margin_excl_fixed_funding,
        ap_retail_margin_incl_fixed_funding
            as ap_retail_margin_incl_fixed_funding,
        trans_conv_rt
        * ap_retail_margin_incl_fixed_funding
            as trans_ap_retail_margin_incl_fixed_funding,
        phi_conv_rt
        * ap_retail_margin_incl_fixed_funding
            as phi_ap_retail_margin_incl_fixed_funding,
        pcomp_conv_rt
        * ap_retail_margin_incl_fixed_funding
            as pcomp_ap_retail_margin_incl_fixed_funding,
        ap_retail_promo_fixed_spend as ap_retail_promo_fixed_spend,
        trans_conv_rt
        * ap_retail_promo_fixed_spend as trans_ap_retail_promo_fixed_spend,
        phi_conv_rt
        * ap_retail_promo_fixed_spend as phi_ap_retail_promo_fixed_spend,
        pcomp_conv_rt
        * ap_retail_promo_fixed_spend as pcomp_ap_retail_promo_fixed_spend,
        ap_retail_retailer_retro_funding as ap_retail_retailer_retro_funding,
        trans_conv_rt
        * ap_retail_retailer_retro_funding
            as trans_ap_retail_retailer_retro_funding,
        phi_conv_rt
        * ap_retail_retailer_retro_funding
            as phi_ap_retail_retailer_retro_funding,
        pcomp_conv_rt
        * ap_retail_retailer_retro_funding
            as pcomp_ap_retail_retailer_retro_funding,
        ap_retail_revenue_mrrsp as ap_retail_revenue_mrrsp,
        trans_conv_rt
        * ap_retail_revenue_mrrsp as trans_ap_retail_revenue_mrrsp,
        phi_conv_rt * ap_retail_revenue_mrrsp as phi_ap_retail_revenue_mrrsp,
        pcomp_conv_rt
        * ap_retail_revenue_mrrsp as pcomp_ap_retail_revenue_mrrsp,
        ap_retail_revenue_net as ap_retail_revenue_net,
        trans_conv_rt * ap_retail_revenue_net as trans_ap_retail_revenue_net,
        phi_conv_rt * ap_retail_revenue_net as phi_ap_retail_revenue_net,
        pcomp_conv_rt * ap_retail_revenue_net as pcomp_ap_retail_revenue_net,
        ap_retail_revenue_net_excl_mrrsp as ap_retail_revenue_net_excl_mrrsp,
        trans_conv_rt
        * ap_retail_revenue_net_excl_mrrsp
            as trans_ap_retail_revenue_net_excl_mrrsp,
        phi_conv_rt
        * ap_retail_revenue_net_excl_mrrsp
            as phi_ap_retail_revenue_net_excl_mrrsp,
        pcomp_conv_rt
        * ap_retail_revenue_net_excl_mrrsp
            as pcomp_ap_retail_revenue_net_excl_mrrsp,
        ap_retail_revenue_net_excl_rsp as ap_retail_revenue_net_excl_rsp,
        trans_conv_rt
        * ap_retail_revenue_net_excl_rsp
            as trans_ap_retail_revenue_net_excl_rsp,
        phi_conv_rt
        * ap_retail_revenue_net_excl_rsp as phi_ap_retail_revenue_net_excl_rsp,
        pcomp_conv_rt
        * ap_retail_revenue_net_excl_rsp
            as pcomp_ap_retail_revenue_net_excl_rsp,
        ap_retail_revenue_rsp as ap_retail_revenue_rsp,
        trans_conv_rt * ap_retail_revenue_rsp as trans_ap_retail_revenue_rsp,
        phi_conv_rt * ap_retail_revenue_rsp as phi_ap_retail_revenue_rsp,
        pcomp_conv_rt * ap_retail_revenue_rsp as pcomp_ap_retail_revenue_rsp,
        ap_retail_total_spend as ap_retail_total_spend,
        trans_conv_rt * ap_retail_total_spend as trans_ap_retail_total_spend,
        phi_conv_rt * ap_retail_total_spend as phi_ap_retail_total_spend,
        pcomp_conv_rt * ap_retail_total_spend as pcomp_ap_retail_total_spend,
        ap_retro as ap_retro,
        trans_conv_rt * ap_retro as trans_ap_retro,
        phi_conv_rt * ap_retro as phi_ap_retro,
        pcomp_conv_rt * ap_retro as pcomp_ap_retro,
        ap_retro_mgmt_adjustment as ap_retro_mgmt_adjustment,
        trans_conv_rt
        * ap_retro_mgmt_adjustment as trans_ap_retro_mgmt_adjustment,
        phi_conv_rt * ap_retro_mgmt_adjustment as phi_ap_retro_mgmt_adjustment,
        pcomp_conv_rt
        * ap_retro_mgmt_adjustment as pcomp_ap_retro_mgmt_adjustment,
        ap_retro_pre_adjustment as ap_retro_pre_adjustment,
        trans_conv_rt
        * ap_retro_pre_adjustment as trans_ap_retro_pre_adjustment,
        phi_conv_rt * ap_retro_pre_adjustment as phi_ap_retro_pre_adjustment,
        pcomp_conv_rt
        * ap_retro_pre_adjustment as pcomp_ap_retro_pre_adjustment,
        ap_tot_prime_cost_standard as ap_tot_prime_cost_standard,
        trans_conv_rt
        * ap_tot_prime_cost_standard as trans_ap_tot_prime_cost_standard,
        phi_conv_rt
        * ap_tot_prime_cost_standard as phi_ap_tot_prime_cost_standard,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard as pcomp_ap_tot_prime_cost_standard,
        ap_tot_prime_cost_standard_mgmt_adjustment
            as ap_tot_prime_cost_standard_mgmt_adjustment,
        trans_conv_rt
        * ap_tot_prime_cost_standard_mgmt_adjustment
            as trans_ap_tot_prime_cost_standard_mgmt_adjustment,
        phi_conv_rt
        * ap_tot_prime_cost_standard_mgmt_adjustment
            as phi_ap_tot_prime_cost_standard_mgmt_adjustment,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard_mgmt_adjustment
            as pcomp_ap_tot_prime_cost_standard_mgmt_adjustment,
        ap_tot_prime_cost_standard_pre_adjustment
            as ap_tot_prime_cost_standard_pre_adjustment,
        trans_conv_rt
        * ap_tot_prime_cost_standard_pre_adjustment
            as trans_ap_tot_prime_cost_standard_pre_adjustment,
        phi_conv_rt
        * ap_tot_prime_cost_standard_pre_adjustment
            as phi_ap_tot_prime_cost_standard_pre_adjustment,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard_pre_adjustment
            as pcomp_ap_tot_prime_cost_standard_pre_adjustment,
        ap_tot_prime_cost_variance as ap_tot_prime_cost_variance,
        trans_conv_rt
        * ap_tot_prime_cost_variance as trans_ap_tot_prime_cost_variance,
        phi_conv_rt
        * ap_tot_prime_cost_variance as phi_ap_tot_prime_cost_variance,
        pcomp_conv_rt
        * ap_tot_prime_cost_variance as pcomp_ap_tot_prime_cost_variance,
        ap_tot_prime_cost_variance_mgmt_adjustment
            as ap_tot_prime_cost_variance_mgmt_adjustment,
        trans_conv_rt
        * ap_tot_prime_cost_variance_mgmt_adjustment
            as trans_ap_tot_prime_cost_variance_mgmt_adjustment,
        phi_conv_rt
        * ap_tot_prime_cost_variance_mgmt_adjustment
            as phi_ap_tot_prime_cost_variance_mgmt_adjustment,
        pcomp_conv_rt
        * ap_tot_prime_cost_variance_mgmt_adjustment
            as pcomp_ap_tot_prime_cost_variance_mgmt_adjustment,
        ap_tot_prime_cost_variance_pre_adjustment
            as ap_tot_prime_cost_variance_pre_adjustment,
        trans_conv_rt
        * ap_tot_prime_cost_variance_pre_adjustment
            as trans_ap_tot_prime_cost_variance_pre_adjustment,
        phi_conv_rt
        * ap_tot_prime_cost_variance_pre_adjustment
            as phi_ap_tot_prime_cost_variance_pre_adjustment,
        pcomp_conv_rt
        * ap_tot_prime_cost_variance_pre_adjustment
            as pcomp_ap_tot_prime_cost_variance_pre_adjustment,
        ap_total_trade as ap_total_trade,
        trans_conv_rt * ap_total_trade as trans_ap_total_trade,
        phi_conv_rt * ap_total_trade as phi_ap_total_trade,
        pcomp_conv_rt * ap_total_trade as pcomp_ap_total_trade,
        ap_total_trade_cust_invoiced as ap_total_trade_cust_invoiced,
        trans_conv_rt
        * ap_total_trade_cust_invoiced as trans_ap_total_trade_cust_invoiced,
        phi_conv_rt
        * ap_total_trade_cust_invoiced as phi_ap_total_trade_cust_invoiced,
        pcomp_conv_rt
        * ap_total_trade_cust_invoiced as pcomp_ap_total_trade_cust_invoiced,
        ap_variable_trade as ap_variable_trade,
        trans_conv_rt * ap_variable_trade as trans_ap_variable_trade,
        phi_conv_rt * ap_variable_trade as phi_ap_variable_trade,
        pcomp_conv_rt * ap_variable_trade as pcomp_ap_variable_trade,
        promo_vol,
        promo_vol_kg,
        promo_vol_ul,
        retail_tot_vol_ca,
        retail_tot_vol_kg,
        retail_tot_vol_sgl,
        retail_tot_vol_sgl_ca,
        retail_tot_vol_sgl_ul,
        retail_tot_vol_sp_base_uom,
        retail_tot_vol_sp_kg_uom,
        retail_tot_vol_sp_ul_uom,
        retail_tot_vol_ul,
        tot_vol_ca,
        tot_vol_kg,
        tot_vol_sgl,
        tot_vol_sgl_ca,
        tot_vol_sgl_ul,
        tot_vol_sp_base_uom,
        tot_vol_sp_base_uom_mgmt_adjustment,
        tot_vol_sp_base_uom_pre_adjustment,
        tot_vol_sp_kg_uom,
        tot_vol_sp_kg_uom_mgmt_adjustment,
        tot_vol_sp_kg_uom_pre_adjustment,
        tot_vol_sp_ul_uom,
        tot_vol_sp_ul_uom_mgmt_adjustment,
        tot_vol_sp_ul_uom_pre_adjustment,
        tot_vol_ul,
        fcf_tot_vol_kg,
        fcf_tot_vol_ca,
        fcf_tot_vol_ul,
        fcf_base_vol_kg,
        fcf_base_vol_ca,
        fcf_base_vol_ul,
        fcf_promo_vol_kg,
        fcf_promo_vol_ca,
        fcf_promo_vol_ul,
        fcf_over_vol_kg,
        fcf_over_vol_ca,
        fcf_over_vol_ul,
        gl_unit_price as gl_unit_price,
        trans_conv_rt * gl_unit_price as trans_gl_unit_price,
        phi_conv_rt * gl_unit_price as phi_gl_unit_price,
        pcomp_conv_rt * gl_unit_price as pcomp_gl_unit_price,
        raw_material_unit_price as raw_material_unit_price,
        trans_conv_rt
        * raw_material_unit_price as trans_raw_material_unit_price,
        phi_conv_rt * raw_material_unit_price as phi_raw_material_unit_price,
        pcomp_conv_rt
        * raw_material_unit_price as pcomp_raw_material_unit_price,
        ap_tot_prime_cost_standard_raw as ap_tot_prime_cost_standard_raw,
        trans_conv_rt
        * ap_tot_prime_cost_standard_raw
            as trans_ap_tot_prime_cost_standard_raw,
        phi_conv_rt
        * ap_tot_prime_cost_standard_raw as phi_ap_tot_prime_cost_standard_raw,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard_raw
            as pcomp_ap_tot_prime_cost_standard_raw,
        packaging_unit_price as packaging_unit_price,
        trans_conv_rt * packaging_unit_price as trans_packaging_unit_price,
        phi_conv_rt * packaging_unit_price as phi_packaging_unit_price,
        pcomp_conv_rt * packaging_unit_price as pcomp_packaging_unit_price,
        ap_tot_prime_cost_standard_packaging
            as ap_tot_prime_cost_standard_packaging,
        trans_conv_rt
        * ap_tot_prime_cost_standard_packaging
            as trans_ap_tot_prime_cost_standard_packaging,
        phi_conv_rt
        * ap_tot_prime_cost_standard_packaging
            as phi_ap_tot_prime_cost_standard_packaging,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard_packaging
            as pcomp_ap_tot_prime_cost_standard_packaging,
        labour_unit_price as labour_unit_price,
        trans_conv_rt * labour_unit_price as trans_labour_unit_price,
        phi_conv_rt * labour_unit_price as phi_labour_unit_price,
        pcomp_conv_rt * labour_unit_price as pcomp_labour_unit_price,
        ap_tot_prime_cost_standard_labour as ap_tot_prime_cost_standard_labour,
        trans_conv_rt
        * ap_tot_prime_cost_standard_labour
            as trans_ap_tot_prime_cost_standard_labour,
        phi_conv_rt
        * ap_tot_prime_cost_standard_labour
            as phi_ap_tot_prime_cost_standard_labour,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard_labour
            as pcomp_ap_tot_prime_cost_standard_labour,
        bought_in_unit_price as bought_in_unit_price,
        trans_conv_rt * bought_in_unit_price as trans_bought_in_unit_price,
        phi_conv_rt * bought_in_unit_price as phi_bought_in_unit_price,
        pcomp_conv_rt * bought_in_unit_price as pcomp_bought_in_unit_price,
        ap_tot_prime_cost_standard_bought_in
            as ap_tot_prime_cost_standard_bought_in,
        trans_conv_rt
        * ap_tot_prime_cost_standard_bought_in
            as trans_ap_tot_prime_cost_standard_bought_in,
        phi_conv_rt
        * ap_tot_prime_cost_standard_bought_in
            as phi_ap_tot_prime_cost_standard_bought_in,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard_bought_in
            as pcomp_ap_tot_prime_cost_standard_bought_in,
        other_unit_price as other_unit_price,
        trans_conv_rt * other_unit_price as trans_other_unit_price,
        phi_conv_rt * other_unit_price as phi_other_unit_price,
        pcomp_conv_rt * other_unit_price as pcomp_other_unit_price,
        ap_tot_prime_cost_standard_other as ap_tot_prime_cost_standard_other,
        trans_conv_rt
        * ap_tot_prime_cost_standard_other
            as trans_ap_tot_prime_cost_standard_other,
        phi_conv_rt
        * ap_tot_prime_cost_standard_other
            as phi_ap_tot_prime_cost_standard_other,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard_other
            as pcomp_ap_tot_prime_cost_standard_other,
        co_pack_unit_price as co_pack_unit_price,
        trans_conv_rt * co_pack_unit_price as trans_co_pack_unit_price,
        phi_conv_rt * co_pack_unit_price as phi_co_pack_unit_price,
        pcomp_conv_rt * co_pack_unit_price as pcomp_co_pack_unit_price,
        ap_tot_prime_cost_standard_co_pack
            as ap_tot_prime_cost_standard_co_pack,
        trans_conv_rt
        * ap_tot_prime_cost_standard_co_pack
            as trans_ap_tot_prime_cost_standard_co_pack,
        phi_conv_rt
        * ap_tot_prime_cost_standard_co_pack
            as phi_ap_tot_prime_cost_standard_co_pack,
        pcomp_conv_rt
        * ap_tot_prime_cost_standard_co_pack
            as pcomp_ap_tot_prime_cost_standard_co_pack,
        ap_tot_consumer_marketing as ap_tot_consumer_marketing,
        trans_conv_rt
        * ap_tot_consumer_marketing
            as trans_ap_tot_consumer_marketing,
        phi_conv_rt
        * ap_tot_consumer_marketing
            as phi_ap_tot_consumer_marketing,
        pcomp_conv_rt
        * ap_tot_consumer_marketing
            as pcomp_ap_tot_consumer_marketing


    from fct_wbx_sls_budget
)





select
    cast(calendar_date as timestamp_ntz(9)) as calendar_date,
    cast(substring(is_vol_total_nonzero, 1, 20) as text(20))
        as is_vol_total_nonzero,
    cast(substring(company_code, 1, 10) as text(10)) as company_code,
    cast(substring(txn_currency, 1, 255) as text(255)) as transaction_currency,
    cast(substring(base_currency, 1, 255) as text(255)) as base_currency,
    cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,
    cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,
    cast(trans_conv_rt as number(29, 9)) as trans_conv_rt,
    cast(base_conv_rt as number(29, 9)) as base_conv_rt,
    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
    cast(substring(isonpromo_si, 1, 20) as text(20)) as isonpromo_si,
    cast(substring(isonpromo_so, 1, 20) as text(20)) as isonpromo_so,
    cast(substring(ispreorpostpromo_si, 1, 20) as text(20))
        as ispreorpostpromo_si,
    cast(substring(ispreorpostpromo_so, 1, 20) as text(20))
        as ispreorpostpromo_so,
    cast(item_guid as text(255)) as item_guid,
    cast(substring(listingactive, 1, 20) as text(20)) as listingactive,
    cast(plan_customer_addr_number_guid as text(255))
        as plan_customer_addr_number_guid,
    cast(substring(plan_source_customer_code, 1, 255) as text(255))
        as plan_source_customer_code,
    cast(qty_ca_cannib_loss_si as number(38, 10)) as qty_ca_cannib_loss_si,
    cast(qty_ca_cannib_loss_so as number(38, 10)) as qty_ca_cannib_loss_so,
    cast(qty_ca_effective_base_fc_si as number(38, 10))
        as qty_ca_effective_base_fc_si,
    cast(qty_ca_effective_base_fc_so as number(38, 10))
        as qty_ca_effective_base_fc_so,
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
    cast(qty_kg_effective_base_fc_si as number(38, 10))
        as qty_kg_effective_base_fc_si,
    cast(qty_kg_effective_base_fc_so as number(38, 10))
        as qty_kg_effective_base_fc_so,
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
    cast(qty_ul_effective_base_fc_si as number(38, 10))
        as qty_ul_effective_base_fc_si,
    cast(qty_ul_effective_base_fc_so as number(38, 10))
        as qty_ul_effective_base_fc_so,
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
    cast(substring(source_item_identifier, 1, 255) as text(255))
        as source_item_identifier,
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(total_baseretentionpercentage as number(38, 10))
        as total_baseretentionpercentage,
    cast(total_si_preorpostdippercentage as number(38, 10))
        as total_si_preorpostdippercentage,
    cast(total_so_preorpostdippercentage as number(38, 10))
        as total_so_preorpostdippercentage,
    cast(ap_added_value_pack as number(38, 10)) as ap_added_value_pack,
    cast(trans_ap_added_value_pack as number(38, 10))
        as trans_ap_added_value_pack,
    cast(phi_ap_added_value_pack as number(38, 10)) as phi_ap_added_value_pack,
    cast(pcomp_ap_added_value_pack as number(38, 10))
        as pcomp_ap_added_value_pack,
    cast(ap_avp_disc as number(38, 10)) as ap_avp_disc,
    cast(trans_ap_avp_disc as number(38, 10)) as trans_ap_avp_disc,
    cast(phi_ap_avp_disc as number(38, 10)) as phi_ap_avp_disc,
    cast(pcomp_ap_avp_disc as number(38, 10)) as pcomp_ap_avp_disc,
    cast(ap_avp_disc_mgmt_adjustment as number(38, 10))
        as ap_avp_disc_mgmt_adjustment,
    cast(trans_ap_avp_disc_mgmt_adjustment as number(38, 10))
        as trans_ap_avp_disc_mgmt_adjustment,
    cast(phi_ap_avp_disc_mgmt_adjustment as number(38, 10))
        as phi_ap_avp_disc_mgmt_adjustment,
    cast(pcomp_ap_avp_disc_mgmt_adjustment as number(38, 10))
        as pcomp_ap_avp_disc_mgmt_adjustment,
    cast(ap_avp_disc_pre_adjustment as number(38, 10))
        as ap_avp_disc_pre_adjustment,
    cast(trans_ap_avp_disc_pre_adjustment as number(38, 10))
        as trans_ap_avp_disc_pre_adjustment,
    cast(phi_ap_avp_disc_pre_adjustment as number(38, 10))
        as phi_ap_avp_disc_pre_adjustment,
    cast(pcomp_ap_avp_disc_pre_adjustment as number(38, 10))
        as pcomp_ap_avp_disc_pre_adjustment,
    cast(ap_cash_disc as number(38, 10)) as ap_cash_disc,
    cast(trans_ap_cash_disc as number(38, 10)) as trans_ap_cash_disc,
    cast(phi_ap_cash_disc as number(38, 10)) as phi_ap_cash_disc,
    cast(pcomp_ap_cash_disc as number(38, 10)) as pcomp_ap_cash_disc,
    cast(ap_cash_disc_mgmt_adjustment as number(38, 10))
        as ap_cash_disc_mgmt_adjustment,
    cast(trans_ap_cash_disc_mgmt_adjustment as number(38, 10))
        as trans_ap_cash_disc_mgmt_adjustment,
    cast(phi_ap_cash_disc_mgmt_adjustment as number(38, 10))
        as phi_ap_cash_disc_mgmt_adjustment,
    cast(pcomp_ap_cash_disc_mgmt_adjustment as number(38, 10))
        as pcomp_ap_cash_disc_mgmt_adjustment,
    cast(ap_cash_disc_pre_adjustment as number(38, 10))
        as ap_cash_disc_pre_adjustment,
    cast(trans_ap_cash_disc_pre_adjustment as number(38, 10))
        as trans_ap_cash_disc_pre_adjustment,
    cast(phi_ap_cash_disc_pre_adjustment as number(38, 10))
        as phi_ap_cash_disc_pre_adjustment,
    cast(pcomp_ap_cash_disc_pre_adjustment as number(38, 10))
        as pcomp_ap_cash_disc_pre_adjustment,
    cast(ap_category as number(38, 10)) as ap_category,
    cast(trans_ap_category as number(38, 10)) as trans_ap_category,
    cast(phi_ap_category as number(38, 10)) as phi_ap_category,
    cast(pcomp_ap_category as number(38, 10)) as pcomp_ap_category,
    cast(ap_category_mgmt_adjustment as number(38, 10))
        as ap_category_mgmt_adjustment,
    cast(trans_ap_category_mgmt_adjustment as number(38, 10))
        as trans_ap_category_mgmt_adjustment,
    cast(phi_ap_category_mgmt_adjustment as number(38, 10))
        as phi_ap_category_mgmt_adjustment,
    cast(pcomp_ap_category_mgmt_adjustment as number(38, 10))
        as pcomp_ap_category_mgmt_adjustment,
    cast(ap_category_pre_adjustment as number(38, 10))
        as ap_category_pre_adjustment,
    cast(trans_ap_category_pre_adjustment as number(38, 10))
        as trans_ap_category_pre_adjustment,
    cast(phi_ap_category_pre_adjustment as number(38, 10))
        as phi_ap_category_pre_adjustment,
    cast(pcomp_ap_category_pre_adjustment as number(38, 10))
        as pcomp_ap_category_pre_adjustment,
    cast(ap_direct_shopper_marketing as number(38, 10))
        as ap_direct_shopper_marketing,
    cast(trans_ap_direct_shopper_marketing as number(38, 10))
        as trans_ap_direct_shopper_marketing,
    cast(phi_ap_direct_shopper_marketing as number(38, 10))
        as phi_ap_direct_shopper_marketing,
    cast(pcomp_ap_direct_shopper_marketing as number(38, 10))
        as pcomp_ap_direct_shopper_marketing,
    cast(ap_direct_shopper_marketing_mgmt_adjustment as number(38, 10))
        as ap_direct_shopper_marketing_mgmt_adjustment,
    cast(trans_ap_direct_shopper_marketing_mgmt_adjustment as number(38, 10))
        as trans_ap_direct_shopper_marketing_mgmt_adjustment,
    cast(phi_ap_direct_shopper_marketing_mgmt_adjustment as number(38, 10))
        as phi_ap_direct_shopper_marketing_mgmt_adjustment,
    cast(pcomp_ap_direct_shopper_marketing_mgmt_adjustment as number(38, 10))
        as pcomp_ap_direct_shopper_marketing_mgmt_adjustment,
    cast(ap_direct_shopper_marketing_pre_adjustment as number(38, 10))
        as ap_direct_shopper_marketing_pre_adjustment,
    cast(trans_ap_direct_shopper_marketing_pre_adjustment as number(38, 10))
        as trans_ap_direct_shopper_marketing_pre_adjustment,
    cast(phi_ap_direct_shopper_marketing_pre_adjustment as number(38, 10))
        as phi_ap_direct_shopper_marketing_pre_adjustment,
    cast(pcomp_ap_direct_shopper_marketing_pre_adjustment as number(38, 10))
        as pcomp_ap_direct_shopper_marketing_pre_adjustment,
    cast(ap_early_settlement_disc as number(38, 10))
        as ap_early_settlement_disc,
    cast(trans_ap_early_settlement_disc as number(38, 10))
        as trans_ap_early_settlement_disc,
    cast(phi_ap_early_settlement_disc as number(38, 10))
        as phi_ap_early_settlement_disc,
    cast(pcomp_ap_early_settlement_disc as number(38, 10))
        as pcomp_ap_early_settlement_disc,
    cast(ap_early_settlement_disc_mgmt_adjustment as number(38, 10))
        as ap_early_settlement_disc_mgmt_adjustment,
    cast(trans_ap_early_settlement_disc_mgmt_adjustment as number(38, 10))
        as trans_ap_early_settlement_disc_mgmt_adjustment,
    cast(phi_ap_early_settlement_disc_mgmt_adjustment as number(38, 10))
        as phi_ap_early_settlement_disc_mgmt_adjustment,
    cast(pcomp_ap_early_settlement_disc_mgmt_adjustment as number(38, 10))
        as pcomp_ap_early_settlement_disc_mgmt_adjustment,
    cast(ap_early_settlement_disc_pre_adjustment as number(38, 10))
        as ap_early_settlement_disc_pre_adjustment,
    cast(trans_ap_early_settlement_disc_pre_adjustment as number(38, 10))
        as trans_ap_early_settlement_disc_pre_adjustment,
    cast(phi_ap_early_settlement_disc_pre_adjustment as number(38, 10))
        as phi_ap_early_settlement_disc_pre_adjustment,
    cast(pcomp_ap_early_settlement_disc_pre_adjustment as number(38, 10))
        as pcomp_ap_early_settlement_disc_pre_adjustment,
    cast(ap_everyday_low_prices as number(38, 10)) as ap_everyday_low_prices,
    cast(trans_ap_everyday_low_prices as number(38, 10))
        as trans_ap_everyday_low_prices,
    cast(phi_ap_everyday_low_prices as number(38, 10))
        as phi_ap_everyday_low_prices,
    cast(pcomp_ap_everyday_low_prices as number(38, 10))
        as pcomp_ap_everyday_low_prices,
    cast(ap_everyday_low_prices_mgmt_adjustment as number(38, 10))
        as ap_everyday_low_prices_mgmt_adjustment,
    cast(trans_ap_everyday_low_prices_mgmt_adjustment as number(38, 10))
        as trans_ap_everyday_low_prices_mgmt_adjustment,
    cast(phi_ap_everyday_low_prices_mgmt_adjustment as number(38, 10))
        as phi_ap_everyday_low_prices_mgmt_adjustment,
    cast(pcomp_ap_everyday_low_prices_mgmt_adjustment as number(38, 10))
        as pcomp_ap_everyday_low_prices_mgmt_adjustment,
    cast(ap_everyday_low_prices_pre_adjustment as number(38, 10))
        as ap_everyday_low_prices_pre_adjustment,
    cast(trans_ap_everyday_low_prices_pre_adjustment as number(38, 10))
        as trans_ap_everyday_low_prices_pre_adjustment,
    cast(phi_ap_everyday_low_prices_pre_adjustment as number(38, 10))
        as phi_ap_everyday_low_prices_pre_adjustment,
    cast(pcomp_ap_everyday_low_prices_pre_adjustment as number(38, 10))
        as pcomp_ap_everyday_low_prices_pre_adjustment,
    cast(ap_field_marketing as number(38, 10)) as ap_field_marketing,
    cast(trans_ap_field_marketing as number(38, 10))
        as trans_ap_field_marketing,
    cast(phi_ap_field_marketing as number(38, 10)) as phi_ap_field_marketing,
    cast(pcomp_ap_field_marketing as number(38, 10))
        as pcomp_ap_field_marketing,
    cast(ap_field_marketing_mgmt_adjustment as number(38, 10))
        as ap_field_marketing_mgmt_adjustment,
    cast(trans_ap_field_marketing_mgmt_adjustment as number(38, 10))
        as trans_ap_field_marketing_mgmt_adjustment,
    cast(phi_ap_field_marketing_mgmt_adjustment as number(38, 10))
        as phi_ap_field_marketing_mgmt_adjustment,
    cast(pcomp_ap_field_marketing_mgmt_adjustment as number(38, 10))
        as pcomp_ap_field_marketing_mgmt_adjustment,
    cast(ap_field_marketing_pre_adjustment as number(38, 10))
        as ap_field_marketing_pre_adjustment,
    cast(trans_ap_field_marketing_pre_adjustment as number(38, 10))
        as trans_ap_field_marketing_pre_adjustment,
    cast(phi_ap_field_marketing_pre_adjustment as number(38, 10))
        as phi_ap_field_marketing_pre_adjustment,
    cast(pcomp_ap_field_marketing_pre_adjustment as number(38, 10))
        as pcomp_ap_field_marketing_pre_adjustment,
    cast(ap_fixed_annual_payments as number(38, 10))
        as ap_fixed_annual_payments,
    cast(trans_ap_fixed_annual_payments as number(38, 10))
        as trans_ap_fixed_annual_payments,
    cast(phi_ap_fixed_annual_payments as number(38, 10))
        as phi_ap_fixed_annual_payments,
    cast(pcomp_ap_fixed_annual_payments as number(38, 10))
        as pcomp_ap_fixed_annual_payments,
    cast(ap_fixed_annual_payments_mgmt_adjustment as number(38, 10))
        as ap_fixed_annual_payments_mgmt_adjustment,
    cast(trans_ap_fixed_annual_payments_mgmt_adjustment as number(38, 10))
        as trans_ap_fixed_annual_payments_mgmt_adjustment,
    cast(phi_ap_fixed_annual_payments_mgmt_adjustment as number(38, 10))
        as phi_ap_fixed_annual_payments_mgmt_adjustment,
    cast(pcomp_ap_fixed_annual_payments_mgmt_adjustment as number(38, 10))
        as pcomp_ap_fixed_annual_payments_mgmt_adjustment,
    cast(ap_fixed_annual_payments_pre_adjustment as number(38, 10))
        as ap_fixed_annual_payments_pre_adjustment,
    cast(trans_ap_fixed_annual_payments_pre_adjustment as number(38, 10))
        as trans_ap_fixed_annual_payments_pre_adjustment,
    cast(phi_ap_fixed_annual_payments_pre_adjustment as number(38, 10))
        as phi_ap_fixed_annual_payments_pre_adjustment,
    cast(pcomp_ap_fixed_annual_payments_pre_adjustment as number(38, 10))
        as pcomp_ap_fixed_annual_payments_pre_adjustment,
    cast(ap_fixed_trade_cust_invoiced as number(38, 10))
        as ap_fixed_trade_cust_invoiced,
    cast(trans_ap_fixed_trade_cust_invoiced as number(38, 10))
        as trans_ap_fixed_trade_cust_invoiced,
    cast(phi_ap_fixed_trade_cust_invoiced as number(38, 10))
        as phi_ap_fixed_trade_cust_invoiced,
    cast(pcomp_ap_fixed_trade_cust_invoiced as number(38, 10))
        as pcomp_ap_fixed_trade_cust_invoiced,
    cast(ap_fixed_trade_non_cust_invoiced as number(38, 10))
        as ap_fixed_trade_non_cust_invoiced,
    cast(trans_ap_fixed_trade_non_cust_invoiced as number(38, 10))
        as trans_ap_fixed_trade_non_cust_invoiced,
    cast(phi_ap_fixed_trade_non_cust_invoiced as number(38, 10))
        as phi_ap_fixed_trade_non_cust_invoiced,
    cast(pcomp_ap_fixed_trade_non_cust_invoiced as number(38, 10))
        as pcomp_ap_fixed_trade_non_cust_invoiced,
    cast(ap_gcat_actuals as number(38, 10)) as ap_gcat_actuals,
    cast(trans_ap_gcat_actuals as number(38, 10)) as trans_ap_gcat_actuals,
    cast(phi_ap_gcat_actuals as number(38, 10)) as phi_ap_gcat_actuals,
    cast(pcomp_ap_gcat_actuals as number(38, 10)) as pcomp_ap_gcat_actuals,
    cast(ap_gcat_standard as number(38, 10)) as ap_gcat_standard,
    cast(trans_ap_gcat_standard as number(38, 10)) as trans_ap_gcat_standard,
    cast(phi_ap_gcat_standard as number(38, 10)) as phi_ap_gcat_standard,
    cast(pcomp_ap_gcat_standard as number(38, 10)) as pcomp_ap_gcat_standard,
    cast(ap_gross_margin_actual as number(38, 10)) as ap_gross_margin_actual,
    cast(trans_ap_gross_margin_actual as number(38, 10))
        as trans_ap_gross_margin_actual,
    cast(phi_ap_gross_margin_actual as number(38, 10))
        as phi_ap_gross_margin_actual,
    cast(pcomp_ap_gross_margin_actual as number(38, 10))
        as pcomp_ap_gross_margin_actual,
    cast(ap_gross_margin_standard as number(38, 10))
        as ap_gross_margin_standard,
    cast(trans_ap_gross_margin_standard as number(38, 10))
        as trans_ap_gross_margin_standard,
    cast(phi_ap_gross_margin_standard as number(38, 10))
        as phi_ap_gross_margin_standard,
    cast(pcomp_ap_gross_margin_standard as number(38, 10))
        as pcomp_ap_gross_margin_standard,
    cast(ap_gross_sales_value as number(38, 10)) as ap_gross_sales_value,
    cast(trans_ap_gross_sales_value as number(38, 10))
        as trans_ap_gross_sales_value,
    cast(phi_ap_gross_sales_value as number(38, 10))
        as phi_ap_gross_sales_value,
    cast(pcomp_ap_gross_sales_value as number(38, 10))
        as pcomp_ap_gross_sales_value,
    cast(ap_gross_selling_value as number(38, 10)) as ap_gross_selling_value,
    cast(trans_ap_gross_selling_value as number(38, 10))
        as trans_ap_gross_selling_value,
    cast(phi_ap_gross_selling_value as number(38, 10))
        as phi_ap_gross_selling_value,
    cast(pcomp_ap_gross_selling_value as number(38, 10))
        as pcomp_ap_gross_selling_value,
    cast(ap_gross_selling_value_mgmt_adjustment as number(38, 10))
        as ap_gross_selling_value_mgmt_adjustment,
    cast(trans_ap_gross_selling_value_mgmt_adjustment as number(38, 10))
        as trans_ap_gross_selling_value_mgmt_adjustment,
    cast(phi_ap_gross_selling_value_mgmt_adjustment as number(38, 10))
        as phi_ap_gross_selling_value_mgmt_adjustment,
    cast(pcomp_ap_gross_selling_value_mgmt_adjustment as number(38, 10))
        as pcomp_ap_gross_selling_value_mgmt_adjustment,
    cast(ap_gross_selling_value_pre_adjustment as number(38, 10))
        as ap_gross_selling_value_pre_adjustment,
    cast(trans_ap_gross_selling_value_pre_adjustment as number(38, 10))
        as trans_ap_gross_selling_value_pre_adjustment,
    cast(phi_ap_gross_selling_value_pre_adjustment as number(38, 10))
        as phi_ap_gross_selling_value_pre_adjustment,
    cast(pcomp_ap_gross_selling_value_pre_adjustment as number(38, 10))
        as pcomp_ap_gross_selling_value_pre_adjustment,
    cast(ap_growth_incentives as number(38, 10)) as ap_growth_incentives,
    cast(trans_ap_growth_incentives as number(38, 10))
        as trans_ap_growth_incentives,
    cast(phi_ap_growth_incentives as number(38, 10))
        as phi_ap_growth_incentives,
    cast(pcomp_ap_growth_incentives as number(38, 10))
        as pcomp_ap_growth_incentives,
    cast(ap_growth_incentives_mgmt_adjustment as number(38, 10))
        as ap_growth_incentives_mgmt_adjustment,
    cast(trans_ap_growth_incentives_mgmt_adjustment as number(38, 10))
        as trans_ap_growth_incentives_mgmt_adjustment,
    cast(phi_ap_growth_incentives_mgmt_adjustment as number(38, 10))
        as phi_ap_growth_incentives_mgmt_adjustment,
    cast(pcomp_ap_growth_incentives_mgmt_adjustment as number(38, 10))
        as pcomp_ap_growth_incentives_mgmt_adjustment,
    cast(ap_growth_incentives_pre_adjustment as number(38, 10))
        as ap_growth_incentives_pre_adjustment,
    cast(trans_ap_growth_incentives_pre_adjustment as number(38, 10))
        as trans_ap_growth_incentives_pre_adjustment,
    cast(phi_ap_growth_incentives_pre_adjustment as number(38, 10))
        as phi_ap_growth_incentives_pre_adjustment,
    cast(pcomp_ap_growth_incentives_pre_adjustment as number(38, 10))
        as pcomp_ap_growth_incentives_pre_adjustment,
    cast(ap_indirect_shopper_marketing as number(38, 10))
        as ap_indirect_shopper_marketing,
    cast(trans_ap_indirect_shopper_marketing as number(38, 10))
        as trans_ap_indirect_shopper_marketing,
    cast(phi_ap_indirect_shopper_marketing as number(38, 10))
        as phi_ap_indirect_shopper_marketing,
    cast(pcomp_ap_indirect_shopper_marketing as number(38, 10))
        as pcomp_ap_indirect_shopper_marketing,
    cast(ap_indirect_shopper_marketing_mgmt_adjustment as number(38, 10))
        as ap_indirect_shopper_marketing_mgmt_adjustment,
    cast(
        trans_ap_indirect_shopper_marketing_mgmt_adjustment as number(38, 10)
    ) as trans_ap_indirect_shopper_marketing_mgmt_adjustment,
    cast(phi_ap_indirect_shopper_marketing_mgmt_adjustment as number(38, 10))
        as phi_ap_indirect_shopper_marketing_mgmt_adjustment,
    cast(
        pcomp_ap_indirect_shopper_marketing_mgmt_adjustment as number(38, 10)
    ) as pcomp_ap_indirect_shopper_marketing_mgmt_adjustment,
    cast(ap_indirect_shopper_marketing_pre_adjustment as number(38, 10))
        as ap_indirect_shopper_marketing_pre_adjustment,
    cast(trans_ap_indirect_shopper_marketing_pre_adjustment as number(38, 10))
        as trans_ap_indirect_shopper_marketing_pre_adjustment,
    cast(phi_ap_indirect_shopper_marketing_pre_adjustment as number(38, 10))
        as phi_ap_indirect_shopper_marketing_pre_adjustment,
    cast(pcomp_ap_indirect_shopper_marketing_pre_adjustment as number(38, 10))
        as pcomp_ap_indirect_shopper_marketing_pre_adjustment,
    cast(ap_invoiced_sales_value as number(38, 10)) as ap_invoiced_sales_value,
    cast(trans_ap_invoiced_sales_value as number(38, 10))
        as trans_ap_invoiced_sales_value,
    cast(phi_ap_invoiced_sales_value as number(38, 10))
        as phi_ap_invoiced_sales_value,
    cast(pcomp_ap_invoiced_sales_value as number(38, 10))
        as pcomp_ap_invoiced_sales_value,
    cast(ap_net_net_sales_value as number(38, 10)) as ap_net_net_sales_value,
    cast(trans_ap_net_net_sales_value as number(38, 10))
        as trans_ap_net_net_sales_value,
    cast(phi_ap_net_net_sales_value as number(38, 10))
        as phi_ap_net_net_sales_value,
    cast(pcomp_ap_net_net_sales_value as number(38, 10))
        as pcomp_ap_net_net_sales_value,
    cast(ap_net_realisable_revenue as number(38, 10))
        as ap_net_realisable_revenue,
    cast(trans_ap_net_realisable_revenue as number(38, 10))
        as trans_ap_net_realisable_revenue,
    cast(phi_ap_net_realisable_revenue as number(38, 10))
        as phi_ap_net_realisable_revenue,
    cast(pcomp_ap_net_realisable_revenue as number(38, 10))
        as pcomp_ap_net_realisable_revenue,
    cast(ap_net_sales_value as number(38, 10)) as ap_net_sales_value,
    cast(trans_ap_net_sales_value as number(38, 10))
        as trans_ap_net_sales_value,
    cast(phi_ap_net_sales_value as number(38, 10)) as phi_ap_net_sales_value,
    cast(pcomp_ap_net_sales_value as number(38, 10))
        as pcomp_ap_net_sales_value,
    cast(ap_off_invoice_disc as number(38, 10)) as ap_off_invoice_disc,
    cast(trans_ap_off_invoice_disc as number(38, 10))
        as trans_ap_off_invoice_disc,
    cast(phi_ap_off_invoice_disc as number(38, 10)) as phi_ap_off_invoice_disc,
    cast(pcomp_ap_off_invoice_disc as number(38, 10))
        as pcomp_ap_off_invoice_disc,
    cast(ap_off_invoice_disc_mgmt_adjustment as number(38, 10))
        as ap_off_invoice_disc_mgmt_adjustment,
    cast(trans_ap_off_invoice_disc_mgmt_adjustment as number(38, 10))
        as trans_ap_off_invoice_disc_mgmt_adjustment,
    cast(phi_ap_off_invoice_disc_mgmt_adjustment as number(38, 10))
        as phi_ap_off_invoice_disc_mgmt_adjustment,
    cast(pcomp_ap_off_invoice_disc_mgmt_adjustment as number(38, 10))
        as pcomp_ap_off_invoice_disc_mgmt_adjustment,
    cast(ap_off_invoice_disc_pre_adjustment as number(38, 10))
        as ap_off_invoice_disc_pre_adjustment,
    cast(trans_ap_off_invoice_disc_pre_adjustment as number(38, 10))
        as trans_ap_off_invoice_disc_pre_adjustment,
    cast(phi_ap_off_invoice_disc_pre_adjustment as number(38, 10))
        as phi_ap_off_invoice_disc_pre_adjustment,
    cast(pcomp_ap_off_invoice_disc_pre_adjustment as number(38, 10))
        as pcomp_ap_off_invoice_disc_pre_adjustment,
    cast(ap_other_direct_payments as number(38, 10))
        as ap_other_direct_payments,
    cast(trans_ap_other_direct_payments as number(38, 10))
        as trans_ap_other_direct_payments,
    cast(phi_ap_other_direct_payments as number(38, 10))
        as phi_ap_other_direct_payments,
    cast(pcomp_ap_other_direct_payments as number(38, 10))
        as pcomp_ap_other_direct_payments,
    cast(ap_other_direct_payments_mgmt_adjustment as number(38, 10))
        as ap_other_direct_payments_mgmt_adjustment,
    cast(trans_ap_other_direct_payments_mgmt_adjustment as number(38, 10))
        as trans_ap_other_direct_payments_mgmt_adjustment,
    cast(phi_ap_other_direct_payments_mgmt_adjustment as number(38, 10))
        as phi_ap_other_direct_payments_mgmt_adjustment,
    cast(pcomp_ap_other_direct_payments_mgmt_adjustment as number(38, 10))
        as pcomp_ap_other_direct_payments_mgmt_adjustment,
    cast(ap_other_direct_payments_pre_adjustment as number(38, 10))
        as ap_other_direct_payments_pre_adjustment,
    cast(trans_ap_other_direct_payments_pre_adjustment as number(38, 10))
        as trans_ap_other_direct_payments_pre_adjustment,
    cast(phi_ap_other_direct_payments_pre_adjustment as number(38, 10))
        as phi_ap_other_direct_payments_pre_adjustment,
    cast(pcomp_ap_other_direct_payments_pre_adjustment as number(38, 10))
        as pcomp_ap_other_direct_payments_pre_adjustment,
    cast(ap_other_indirect_payments as number(38, 10))
        as ap_other_indirect_payments,
    cast(trans_ap_other_indirect_payments as number(38, 10))
        as trans_ap_other_indirect_payments,
    cast(phi_ap_other_indirect_payments as number(38, 10))
        as phi_ap_other_indirect_payments,
    cast(pcomp_ap_other_indirect_payments as number(38, 10))
        as pcomp_ap_other_indirect_payments,
    cast(ap_other_indirect_payments_mgmt_adjustment as number(38, 10))
        as ap_other_indirect_payments_mgmt_adjustment,
    cast(trans_ap_other_indirect_payments_mgmt_adjustment as number(38, 10))
        as trans_ap_other_indirect_payments_mgmt_adjustment,
    cast(phi_ap_other_indirect_payments_mgmt_adjustment as number(38, 10))
        as phi_ap_other_indirect_payments_mgmt_adjustment,
    cast(pcomp_ap_other_indirect_payments_mgmt_adjustment as number(38, 10))
        as pcomp_ap_other_indirect_payments_mgmt_adjustment,
    cast(ap_other_indirect_payments_pre_adjustment as number(38, 10))
        as ap_other_indirect_payments_pre_adjustment,
    cast(trans_ap_other_indirect_payments_pre_adjustment as number(38, 10))
        as trans_ap_other_indirect_payments_pre_adjustment,
    cast(phi_ap_other_indirect_payments_pre_adjustment as number(38, 10))
        as phi_ap_other_indirect_payments_pre_adjustment,
    cast(pcomp_ap_other_indirect_payments_pre_adjustment as number(38, 10))
        as pcomp_ap_other_indirect_payments_pre_adjustment,
    cast(ap_permanent_disc as number(38, 10)) as ap_permanent_disc,
    cast(trans_ap_permanent_disc as number(38, 10)) as trans_ap_permanent_disc,
    cast(phi_ap_permanent_disc as number(38, 10)) as phi_ap_permanent_disc,
    cast(pcomp_ap_permanent_disc as number(38, 10)) as pcomp_ap_permanent_disc,
    cast(ap_promo_fixed_funding as number(38, 10)) as ap_promo_fixed_funding,
    cast(trans_ap_promo_fixed_funding as number(38, 10))
        as trans_ap_promo_fixed_funding,
    cast(phi_ap_promo_fixed_funding as number(38, 10))
        as phi_ap_promo_fixed_funding,
    cast(pcomp_ap_promo_fixed_funding as number(38, 10))
        as pcomp_ap_promo_fixed_funding,
    cast(ap_promo_fixed_funding_mgmt_adjustment as number(38, 10))
        as ap_promo_fixed_funding_mgmt_adjustment,
    cast(trans_ap_promo_fixed_funding_mgmt_adjustment as number(38, 10))
        as trans_ap_promo_fixed_funding_mgmt_adjustment,
    cast(phi_ap_promo_fixed_funding_mgmt_adjustment as number(38, 10))
        as phi_ap_promo_fixed_funding_mgmt_adjustment,
    cast(pcomp_ap_promo_fixed_funding_mgmt_adjustment as number(38, 10))
        as pcomp_ap_promo_fixed_funding_mgmt_adjustment,
    cast(ap_promo_fixed_funding_pre_adjustment as number(38, 10))
        as ap_promo_fixed_funding_pre_adjustment,
    cast(trans_ap_promo_fixed_funding_pre_adjustment as number(38, 10))
        as trans_ap_promo_fixed_funding_pre_adjustment,
    cast(phi_ap_promo_fixed_funding_pre_adjustment as number(38, 10))
        as phi_ap_promo_fixed_funding_pre_adjustment,
    cast(pcomp_ap_promo_fixed_funding_pre_adjustment as number(38, 10))
        as pcomp_ap_promo_fixed_funding_pre_adjustment,
    cast(ap_range_support_allowance as number(38, 10))
        as ap_range_support_allowance,
    cast(trans_ap_range_support_allowance as number(38, 10))
        as trans_ap_range_support_allowance,
    cast(phi_ap_range_support_allowance as number(38, 10))
        as phi_ap_range_support_allowance,
    cast(pcomp_ap_range_support_allowance as number(38, 10))
        as pcomp_ap_range_support_allowance,
    cast(ap_range_support_allowance_mgmt_adjustment as number(38, 10))
        as ap_range_support_allowance_mgmt_adjustment,
    cast(trans_ap_range_support_allowance_mgmt_adjustment as number(38, 10))
        as trans_ap_range_support_allowance_mgmt_adjustment,
    cast(phi_ap_range_support_allowance_mgmt_adjustment as number(38, 10))
        as phi_ap_range_support_allowance_mgmt_adjustment,
    cast(pcomp_ap_range_support_allowance_mgmt_adjustment as number(38, 10))
        as pcomp_ap_range_support_allowance_mgmt_adjustment,
    cast(ap_range_support_allowance_pre_adjustment as number(38, 10))
        as ap_range_support_allowance_pre_adjustment,
    cast(trans_ap_range_support_allowance_pre_adjustment as number(38, 10))
        as trans_ap_range_support_allowance_pre_adjustment,
    cast(phi_ap_range_support_allowance_pre_adjustment as number(38, 10))
        as phi_ap_range_support_allowance_pre_adjustment,
    cast(pcomp_ap_range_support_allowance_pre_adjustment as number(38, 10))
        as pcomp_ap_range_support_allowance_pre_adjustment,
    cast(ap_range_support_incentives as number(38, 10))
        as ap_range_support_incentives,
    cast(trans_ap_range_support_incentives as number(38, 10))
        as trans_ap_range_support_incentives,
    cast(phi_ap_range_support_incentives as number(38, 10))
        as phi_ap_range_support_incentives,
    cast(pcomp_ap_range_support_incentives as number(38, 10))
        as pcomp_ap_range_support_incentives,
    cast(ap_range_support_incentives_mgmt_adjustment as number(38, 10))
        as ap_range_support_incentives_mgmt_adjustment,
    cast(trans_ap_range_support_incentives_mgmt_adjustment as number(38, 10))
        as trans_ap_range_support_incentives_mgmt_adjustment,
    cast(phi_ap_range_support_incentives_mgmt_adjustment as number(38, 10))
        as phi_ap_range_support_incentives_mgmt_adjustment,
    cast(pcomp_ap_range_support_incentives_mgmt_adjustment as number(38, 10))
        as pcomp_ap_range_support_incentives_mgmt_adjustment,
    cast(ap_range_support_incentives_pre_adjustment as number(38, 10))
        as ap_range_support_incentives_pre_adjustment,
    cast(trans_ap_range_support_incentives_pre_adjustment as number(38, 10))
        as trans_ap_range_support_incentives_pre_adjustment,
    cast(phi_ap_range_support_incentives_pre_adjustment as number(38, 10))
        as phi_ap_range_support_incentives_pre_adjustment,
    cast(pcomp_ap_range_support_incentives_pre_adjustment as number(38, 10))
        as pcomp_ap_range_support_incentives_pre_adjustment,
    cast(ap_retail_cost_of_sales as number(38, 10)) as ap_retail_cost_of_sales,
    cast(trans_ap_retail_cost_of_sales as number(38, 10))
        as trans_ap_retail_cost_of_sales,
    cast(phi_ap_retail_cost_of_sales as number(38, 10))
        as phi_ap_retail_cost_of_sales,
    cast(pcomp_ap_retail_cost_of_sales as number(38, 10))
        as pcomp_ap_retail_cost_of_sales,
    cast(ap_retail_margin_excl_fixed_funding as number(38, 10))
        as ap_retail_margin_excl_fixed_funding,
    cast(trans_ap_retail_margin_excl_fixed_funding as number(38, 10))
        as trans_ap_retail_margin_excl_fixed_funding,
    cast(phi_ap_retail_margin_excl_fixed_funding as number(38, 10))
        as phi_ap_retail_margin_excl_fixed_funding,
    cast(pcomp_ap_retail_margin_excl_fixed_funding as number(38, 10))
        as pcomp_ap_retail_margin_excl_fixed_funding,
    cast(ap_retail_margin_incl_fixed_funding as number(38, 10))
        as ap_retail_margin_incl_fixed_funding,
    cast(trans_ap_retail_margin_incl_fixed_funding as number(38, 10))
        as trans_ap_retail_margin_incl_fixed_funding,
    cast(phi_ap_retail_margin_incl_fixed_funding as number(38, 10))
        as phi_ap_retail_margin_incl_fixed_funding,
    cast(pcomp_ap_retail_margin_incl_fixed_funding as number(38, 10))
        as pcomp_ap_retail_margin_incl_fixed_funding,
    cast(ap_retail_promo_fixed_spend as number(38, 10))
        as ap_retail_promo_fixed_spend,
    cast(trans_ap_retail_promo_fixed_spend as number(38, 10))
        as trans_ap_retail_promo_fixed_spend,
    cast(phi_ap_retail_promo_fixed_spend as number(38, 10))
        as phi_ap_retail_promo_fixed_spend,
    cast(pcomp_ap_retail_promo_fixed_spend as number(38, 10))
        as pcomp_ap_retail_promo_fixed_spend,
    cast(ap_retail_retailer_retro_funding as number(38, 10))
        as ap_retail_retailer_retro_funding,
    cast(trans_ap_retail_retailer_retro_funding as number(38, 10))
        as trans_ap_retail_retailer_retro_funding,
    cast(phi_ap_retail_retailer_retro_funding as number(38, 10))
        as phi_ap_retail_retailer_retro_funding,
    cast(pcomp_ap_retail_retailer_retro_funding as number(38, 10))
        as pcomp_ap_retail_retailer_retro_funding,
    cast(ap_retail_revenue_mrrsp as number(38, 10)) as ap_retail_revenue_mrrsp,
    cast(trans_ap_retail_revenue_mrrsp as number(38, 10))
        as trans_ap_retail_revenue_mrrsp,
    cast(phi_ap_retail_revenue_mrrsp as number(38, 10))
        as phi_ap_retail_revenue_mrrsp,
    cast(pcomp_ap_retail_revenue_mrrsp as number(38, 10))
        as pcomp_ap_retail_revenue_mrrsp,
    cast(ap_retail_revenue_net as number(38, 10)) as ap_retail_revenue_net,
    cast(trans_ap_retail_revenue_net as number(38, 10))
        as trans_ap_retail_revenue_net,
    cast(phi_ap_retail_revenue_net as number(38, 10))
        as phi_ap_retail_revenue_net,
    cast(pcomp_ap_retail_revenue_net as number(38, 10))
        as pcomp_ap_retail_revenue_net,
    cast(ap_retail_revenue_net_excl_mrrsp as number(38, 10))
        as ap_retail_revenue_net_excl_mrrsp,
    cast(trans_ap_retail_revenue_net_excl_mrrsp as number(38, 10))
        as trans_ap_retail_revenue_net_excl_mrrsp,
    cast(phi_ap_retail_revenue_net_excl_mrrsp as number(38, 10))
        as phi_ap_retail_revenue_net_excl_mrrsp,
    cast(pcomp_ap_retail_revenue_net_excl_mrrsp as number(38, 10))
        as pcomp_ap_retail_revenue_net_excl_mrrsp,
    cast(ap_retail_revenue_net_excl_rsp as number(38, 10))
        as ap_retail_revenue_net_excl_rsp,
    cast(trans_ap_retail_revenue_net_excl_rsp as number(38, 10))
        as trans_ap_retail_revenue_net_excl_rsp,
    cast(phi_ap_retail_revenue_net_excl_rsp as number(38, 10))
        as phi_ap_retail_revenue_net_excl_rsp,
    cast(pcomp_ap_retail_revenue_net_excl_rsp as number(38, 10))
        as pcomp_ap_retail_revenue_net_excl_rsp,
    cast(ap_retail_revenue_rsp as number(38, 10)) as ap_retail_revenue_rsp,
    cast(trans_ap_retail_revenue_rsp as number(38, 10))
        as trans_ap_retail_revenue_rsp,
    cast(phi_ap_retail_revenue_rsp as number(38, 10))
        as phi_ap_retail_revenue_rsp,
    cast(pcomp_ap_retail_revenue_rsp as number(38, 10))
        as pcomp_ap_retail_revenue_rsp,
    cast(ap_retail_total_spend as number(38, 10)) as ap_retail_total_spend,
    cast(trans_ap_retail_total_spend as number(38, 10))
        as trans_ap_retail_total_spend,
    cast(phi_ap_retail_total_spend as number(38, 10))
        as phi_ap_retail_total_spend,
    cast(pcomp_ap_retail_total_spend as number(38, 10))
        as pcomp_ap_retail_total_spend,
    cast(ap_retro as number(38, 10)) as ap_retro,
    cast(trans_ap_retro as number(38, 10)) as trans_ap_retro,
    cast(phi_ap_retro as number(38, 10)) as phi_ap_retro,
    cast(pcomp_ap_retro as number(38, 10)) as pcomp_ap_retro,
    cast(ap_retro_mgmt_adjustment as number(38, 10))
        as ap_retro_mgmt_adjustment,
    cast(trans_ap_retro_mgmt_adjustment as number(38, 10))
        as trans_ap_retro_mgmt_adjustment,
    cast(phi_ap_retro_mgmt_adjustment as number(38, 10))
        as phi_ap_retro_mgmt_adjustment,
    cast(pcomp_ap_retro_mgmt_adjustment as number(38, 10))
        as pcomp_ap_retro_mgmt_adjustment,
    cast(ap_retro_pre_adjustment as number(38, 10)) as ap_retro_pre_adjustment,
    cast(trans_ap_retro_pre_adjustment as number(38, 10))
        as trans_ap_retro_pre_adjustment,
    cast(phi_ap_retro_pre_adjustment as number(38, 10))
        as phi_ap_retro_pre_adjustment,
    cast(pcomp_ap_retro_pre_adjustment as number(38, 10))
        as pcomp_ap_retro_pre_adjustment,
    cast(ap_tot_prime_cost_standard as number(38, 10))
        as ap_tot_prime_cost_standard,
    cast(trans_ap_tot_prime_cost_standard as number(38, 10))
        as trans_ap_tot_prime_cost_standard,
    cast(phi_ap_tot_prime_cost_standard as number(38, 10))
        as phi_ap_tot_prime_cost_standard,
    cast(pcomp_ap_tot_prime_cost_standard as number(38, 10))
        as pcomp_ap_tot_prime_cost_standard,
    cast(ap_tot_prime_cost_standard_mgmt_adjustment as number(38, 10))
        as ap_tot_prime_cost_standard_mgmt_adjustment,
    cast(trans_ap_tot_prime_cost_standard_mgmt_adjustment as number(38, 10))
        as trans_ap_tot_prime_cost_standard_mgmt_adjustment,
    cast(phi_ap_tot_prime_cost_standard_mgmt_adjustment as number(38, 10))
        as phi_ap_tot_prime_cost_standard_mgmt_adjustment,
    cast(pcomp_ap_tot_prime_cost_standard_mgmt_adjustment as number(38, 10))
        as pcomp_ap_tot_prime_cost_standard_mgmt_adjustment,
    cast(ap_tot_prime_cost_standard_pre_adjustment as number(38, 10))
        as ap_tot_prime_cost_standard_pre_adjustment,
    cast(trans_ap_tot_prime_cost_standard_pre_adjustment as number(38, 10))
        as trans_ap_tot_prime_cost_standard_pre_adjustment,
    cast(phi_ap_tot_prime_cost_standard_pre_adjustment as number(38, 10))
        as phi_ap_tot_prime_cost_standard_pre_adjustment,
    cast(pcomp_ap_tot_prime_cost_standard_pre_adjustment as number(38, 10))
        as pcomp_ap_tot_prime_cost_standard_pre_adjustment,
    cast(ap_tot_prime_cost_variance as number(38, 10))
        as ap_tot_prime_cost_variance,
    cast(trans_ap_tot_prime_cost_variance as number(38, 10))
        as trans_ap_tot_prime_cost_variance,
    cast(phi_ap_tot_prime_cost_variance as number(38, 10))
        as phi_ap_tot_prime_cost_variance,
    cast(pcomp_ap_tot_prime_cost_variance as number(38, 10))
        as pcomp_ap_tot_prime_cost_variance,
    cast(ap_tot_prime_cost_variance_mgmt_adjustment as number(38, 10))
        as ap_tot_prime_cost_variance_mgmt_adjustment,
    cast(trans_ap_tot_prime_cost_variance_mgmt_adjustment as number(38, 10))
        as trans_ap_tot_prime_cost_variance_mgmt_adjustment,
    cast(phi_ap_tot_prime_cost_variance_mgmt_adjustment as number(38, 10))
        as phi_ap_tot_prime_cost_variance_mgmt_adjustment,
    cast(pcomp_ap_tot_prime_cost_variance_mgmt_adjustment as number(38, 10))
        as pcomp_ap_tot_prime_cost_variance_mgmt_adjustment,
    cast(ap_tot_prime_cost_variance_pre_adjustment as number(38, 10))
        as ap_tot_prime_cost_variance_pre_adjustment,
    cast(trans_ap_tot_prime_cost_variance_pre_adjustment as number(38, 10))
        as trans_ap_tot_prime_cost_variance_pre_adjustment,
    cast(phi_ap_tot_prime_cost_variance_pre_adjustment as number(38, 10))
        as phi_ap_tot_prime_cost_variance_pre_adjustment,
    cast(pcomp_ap_tot_prime_cost_variance_pre_adjustment as number(38, 10))
        as pcomp_ap_tot_prime_cost_variance_pre_adjustment,
    cast(ap_total_trade as number(38, 10)) as ap_total_trade,
    cast(trans_ap_total_trade as number(38, 10)) as trans_ap_total_trade,
    cast(phi_ap_total_trade as number(38, 10)) as phi_ap_total_trade,
    cast(pcomp_ap_total_trade as number(38, 10)) as pcomp_ap_total_trade,
    cast(ap_total_trade_cust_invoiced as number(38, 10))
        as ap_total_trade_cust_invoiced,
    cast(trans_ap_total_trade_cust_invoiced as number(38, 10))
        as trans_ap_total_trade_cust_invoiced,
    cast(phi_ap_total_trade_cust_invoiced as number(38, 10))
        as phi_ap_total_trade_cust_invoiced,
    cast(pcomp_ap_total_trade_cust_invoiced as number(38, 10))
        as pcomp_ap_total_trade_cust_invoiced,
    cast(ap_variable_trade as number(38, 10)) as ap_variable_trade,
    cast(trans_ap_variable_trade as number(38, 10)) as trans_ap_variable_trade,
    cast(phi_ap_variable_trade as number(38, 10)) as phi_ap_variable_trade,
    cast(pcomp_ap_variable_trade as number(38, 10)) as pcomp_ap_variable_trade,
    cast(promo_vol as number(38, 10)) as promo_vol,
    cast(promo_vol_kg as number(38, 10)) as promo_vol_kg,
    cast(promo_vol_ul as number(38, 10)) as promo_vol_ul,
    cast(retail_tot_vol_ca as number(38, 10)) as retail_tot_vol_ca,
    cast(retail_tot_vol_kg as number(38, 10)) as retail_tot_vol_kg,
    cast(retail_tot_vol_sgl as number(38, 10)) as retail_tot_vol_sgl,
    cast(retail_tot_vol_sgl_ca as number(38, 10)) as retail_tot_vol_sgl_ca,
    cast(retail_tot_vol_sgl_ul as number(38, 10)) as retail_tot_vol_sgl_ul,
    cast(retail_tot_vol_sp_base_uom as number(38, 10))
        as retail_tot_vol_sp_base_uom,
    cast(retail_tot_vol_sp_kg_uom as number(38, 10))
        as retail_tot_vol_sp_kg_uom,
    cast(retail_tot_vol_sp_ul_uom as number(38, 10))
        as retail_tot_vol_sp_ul_uom,
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
    cast(trans_gl_unit_price as float) as trans_gl_unit_price,
    cast(phi_gl_unit_price as float) as phi_gl_unit_price,
    cast(pcomp_gl_unit_price as float) as pcomp_gl_unit_price,
    cast(raw_material_unit_price as float) as raw_material_unit_price,
    cast(trans_raw_material_unit_price as float)
        as trans_raw_material_unit_price,
    cast(phi_raw_material_unit_price as float) as phi_raw_material_unit_price,
    cast(pcomp_raw_material_unit_price as float)
        as pcomp_raw_material_unit_price,
    cast(ap_tot_prime_cost_standard_raw as float)
        as ap_tot_prime_cost_standard_raw,
    cast(trans_ap_tot_prime_cost_standard_raw as float)
        as trans_ap_tot_prime_cost_standard_raw,
    cast(phi_ap_tot_prime_cost_standard_raw as float)
        as phi_ap_tot_prime_cost_standard_raw,
    cast(pcomp_ap_tot_prime_cost_standard_raw as float)
        as pcomp_ap_tot_prime_cost_standard_raw,
    cast(packaging_unit_price as float) as packaging_unit_price,
    cast(trans_packaging_unit_price as float) as trans_packaging_unit_price,
    cast(phi_packaging_unit_price as float) as phi_packaging_unit_price,
    cast(pcomp_packaging_unit_price as float) as pcomp_packaging_unit_price,
    cast(ap_tot_prime_cost_standard_packaging as float)
        as ap_tot_prime_cost_standard_packaging,
    cast(trans_ap_tot_prime_cost_standard_packaging as float)
        as trans_ap_tot_prime_cost_standard_packaging,
    cast(phi_ap_tot_prime_cost_standard_packaging as float)
        as phi_ap_tot_prime_cost_standard_packaging,
    cast(pcomp_ap_tot_prime_cost_standard_packaging as float)
        as pcomp_ap_tot_prime_cost_standard_packaging,
    cast(labour_unit_price as float) as labour_unit_price,
    cast(trans_labour_unit_price as float) as trans_labour_unit_price,
    cast(phi_labour_unit_price as float) as phi_labour_unit_price,
    cast(pcomp_labour_unit_price as float) as pcomp_labour_unit_price,
    cast(ap_tot_prime_cost_standard_labour as float)
        as ap_tot_prime_cost_standard_labour,
    cast(trans_ap_tot_prime_cost_standard_labour as float)
        as trans_ap_tot_prime_cost_standard_labour,
    cast(phi_ap_tot_prime_cost_standard_labour as float)
        as phi_ap_tot_prime_cost_standard_labour,
    cast(pcomp_ap_tot_prime_cost_standard_labour as float)
        as pcomp_ap_tot_prime_cost_standard_labour,
    cast(bought_in_unit_price as float) as bought_in_unit_price,
    cast(trans_bought_in_unit_price as float) as trans_bought_in_unit_price,
    cast(phi_bought_in_unit_price as float) as phi_bought_in_unit_price,
    cast(pcomp_bought_in_unit_price as float) as pcomp_bought_in_unit_price,
    cast(ap_tot_prime_cost_standard_bought_in as float)
        as ap_tot_prime_cost_standard_bought_in,
    cast(trans_ap_tot_prime_cost_standard_bought_in as float)
        as trans_ap_tot_prime_cost_standard_bought_in,
    cast(phi_ap_tot_prime_cost_standard_bought_in as float)
        as phi_ap_tot_prime_cost_standard_bought_in,
    cast(pcomp_ap_tot_prime_cost_standard_bought_in as float)
        as pcomp_ap_tot_prime_cost_standard_bought_in,
    cast(other_unit_price as float) as other_unit_price,
    cast(trans_other_unit_price as float) as trans_other_unit_price,
    cast(phi_other_unit_price as float) as phi_other_unit_price,
    cast(pcomp_other_unit_price as float) as pcomp_other_unit_price,
    cast(ap_tot_prime_cost_standard_other as float)
        as ap_tot_prime_cost_standard_other,
    cast(trans_ap_tot_prime_cost_standard_other as float)
        as trans_ap_tot_prime_cost_standard_other,
    cast(phi_ap_tot_prime_cost_standard_other as float)
        as phi_ap_tot_prime_cost_standard_other,
    cast(pcomp_ap_tot_prime_cost_standard_other as float)
        as pcomp_ap_tot_prime_cost_standard_other,
    cast(co_pack_unit_price as float) as co_pack_unit_price,
    cast(trans_co_pack_unit_price as float) as trans_co_pack_unit_price,
    cast(phi_co_pack_unit_price as float) as phi_co_pack_unit_price,
    cast(pcomp_co_pack_unit_price as float) as pcomp_co_pack_unit_price,
    cast(ap_tot_prime_cost_standard_co_pack as float)
        as ap_tot_prime_cost_standard_co_pack,
    cast(trans_ap_tot_prime_cost_standard_co_pack as float)
        as trans_ap_tot_prime_cost_standard_co_pack,
    cast(phi_ap_tot_prime_cost_standard_co_pack as float)
        as phi_ap_tot_prime_cost_standard_co_pack,
    cast(pcomp_ap_tot_prime_cost_standard_co_pack as float)
        as pcomp_ap_tot_prime_cost_standard_co_pack,
    cast(ap_tot_consumer_marketing as float)
        as ap_tot_consumer_marketing,
    cast(trans_ap_tot_consumer_marketing as float)
        as trans_ap_tot_consumer_marketing,
    cast(phi_ap_tot_consumer_marketing as float)
        as phi_ap_tot_consumer_marketing,
    cast(pcomp_ap_tot_consumer_marketing as float)
        as pcomp_ap_tot_consumer_marketing,
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
from curr_conv