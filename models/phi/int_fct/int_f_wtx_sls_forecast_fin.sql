{{
    config(
        tags = ["sls","sales","forecast","sls_forecast","sls_finance"]
    )
}}

/*  For the Forecast data here, the CBOM is required to calculating the PCOS Std similar to how it is handled for Sales Orders.
    However, for D365 and IBE specfically, the PCOS Std does not need to be calculated using the CBOM.  So this model is not used for IBE and 
    so there are no changes required here for D365 BR1.  
    For BR2 work, and WBX specifically, there may be a need to update this around the CBOM.  But it could work as is too.

*/

with sls_forecast_fin as (
    select * from {{ ref('stg_f_wtx_sls_forecast_fin')}}
),
customer_master_ext as (
    select distinct 
        trim(trade_type_code)       as trade_type_code ,
        trade_type_desc,
        trade_type_seq,
        market_code,
        market_desc,
        market_code_seq,
        sub_market_code,
        sub_market_desc,
        sub_market_code_seq,
        trade_class_code,
        trade_class_desc,
        trade_class_seq,
        trade_group_code,
        trade_group_desc,
        trade_group_seq,
        trade_sector_code,
        trade_sector_desc,
        trade_sector_seq
    from {{ ref('dim_wbx_customer_ext')}}
    where (trim(trade_type_code) is not null and  trim(trade_type_code)!='')
    order by trade_type_code
),
snapshot_date as (
    select snapshot_date from (
    select *,rank() over (order by snapshot_date desc) rank 
    from {{ ref('stg_d_wtx_lkp_snapshot_date')}}) where rank=1
),
scenario_master as (
    select source_system,
    cast(scenario_guid as text(255) )  as scenario_guid,scenario_id,scenario_code,scenario_desc 
    from {{ ref('dim_wbx_scenario')}} where source_system='{{env_var("DBT_SOURCE_SYSTEM")}}'
),
item_master as (
    select distinct source_system,item_guid,source_item_identifier 
    from {{ ref('dim_wbx_item')}} where source_system='{{env_var("DBT_SOURCE_SYSTEM")}}'
),
customer_planning as (
    select trade_type_code 
    from {{ ref('stg_d_wbx_customer_planning')}}
),
cbom as (
    select * from {{ ref('int_f_wbx_mfg_cbom_forecast')}}
),
stage as (
    select 
        source_system,
        sku_idx,
        source_item_identifier,
        {{dbt_utils.surrogate_key(["sls.source_system",
            "ltrim(rtrim(sls.source_item_identifier))",])}}  as item_guid,
        cust_idx,
        plan_source_customer_code,
        calendar_date,
        scen_idx,
        scen_code,
        scen_name,
        decode(upper(isonpromo_si),'FALSE','N','TRUE','Y') as isonpromo_si,
        decode(upper(isonpromo_so),'FALSE','N','TRUE','Y') as isonpromo_so,
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
        ap_retail_revenue_net_excl_rsp,
        trade_type_code ,
        trade_type_desc,
        trade_type_seq,
        market_code,
        market_desc,
        market_code_seq,
        sub_market_code,
        sub_market_desc,
        sub_market_code_seq,
        trade_class_code,
        trade_class_desc,
        trade_class_seq,
        trade_group_code,
        trade_group_desc,
        trade_group_seq,
        trade_sector_code,
        trade_sector_desc,
        trade_sector_seq,
        snapshot_date
    from sls_forecast_fin sls 
    left outer join customer_master_ext plan_cust
on trim(sls.plan_source_customer_code) = trim(plan_cust.trade_type_code)
left join snapshot_date  snap on 1=1
),
uom_conversion as (
        select
            stg.source_system                               as source_system,
            sku_idx                                         as sku_idx,
            stg.source_item_identifier,
            stg.item_guid                                   as item_guid,
            cust_idx,
            cust_plan_lkp.trade_type_code,
            plan_source_customer_code,
            0                                               as customer_address_number_guid,
            snapshot_date,
            calendar_date,
            scen_idx,
            stg.scen_code,
            stg.scen_name,
            case when stg.scen_code='HISTORY' then '0' 
            else scenario_guid    end                       as  scenario_guid,
            isonpromo_si,
            isonpromo_so,
            uom_ca_kg_lkp.conversion_rate                   as v_ca_kg_conv,
            uom_ca_pl_lkp.conversion_rate                   as v_ca_pl_conv,
            uom_kg_ca_lkp.conversion_rate                   as v_kg_ca_conv,
            tot_vol_sp_base_uom,
            tot_vol_sp_base_uom*v_ca_kg_conv                as tot_vol_sp_kg_uom,
            tot_vol_sp_base_uom*v_ca_pl_conv                as tot_vol_sp_ul_uom,
            tot_vol_sp_base_uom_pre_adjustment,
            tot_vol_sp_base_uom_pre_adjustment*v_ca_kg_conv as tot_vol_sp_kg_uom_pre_adjustment,
            tot_vol_sp_base_uom_pre_adjustment*v_ca_pl_conv as tot_vol_sp_ul_uom_pre_adjustment,
            tot_vol_sp_base_uom_mgmt_adjustment,
            tot_vol_sp_base_uom_mgmt_adjustment*v_ca_kg_conv as tot_vol_sp_kg_uom_mgmt_adjustment,
            tot_vol_sp_base_uom_mgmt_adjustment*v_ca_pl_conv as tot_vol_sp_ul_uom_mgmt_adjustment,
            retail_tot_vol_sp_base_uom,
            retail_tot_vol_sp_base_uom*v_ca_kg_conv          as retail_tot_vol_sp_kg_uom,
            retail_tot_vol_sp_base_uom*v_ca_pl_conv          as retail_tot_vol_sp_ul_uom,
            promo_vol,
            promo_vol*v_ca_kg_conv                           as promo_vol_kg,
            promo_vol*v_ca_pl_conv                           as promo_vol_ul,
            tot_vol_sgl,
            0                                                as tot_vol_sgl_ca,
            0                                                as tot_vol_sgl_ul,
            tot_vol_kg,
            tot_vol_kg*v_kg_ca_conv                          as tot_vol_ca,
            tot_vol_ca*v_ca_pl_conv                          as tot_vol_ul,
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
            0                                                       as retail_tot_vol_sgl_ca,
            0                                                       as retail_tot_vol_sgl_ul,
            retail_tot_vol_kg,
            retail_tot_vol_kg*v_kg_ca_conv                          as retail_tot_vol_ca,
            retail_tot_vol_ca*v_ca_pl_conv                          as retail_tot_vol_ul,
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
            ap_retail_revenue_net_excl_rsp,
            0                                                       as gl_unit_price,
            0                                                       as raw_material_unit_price,
            0                                                       as ap_tot_prime_cost_standard_raw,
            0                                                       as packaging_unit_price,
            0                                                       as ap_tot_prime_cost_standard_packaging,
            0                                                       as labour_unit_price,
            0                                                       as ap_tot_prime_cost_standard_labour,
            0                                                       as bought_in_unit_price,
            0                                                       as ap_tot_prime_cost_standard_bought_in,
            0                                                       as other_unit_price,
            0                                                       as ap_tot_prime_cost_standard_other,
            0                                                       as co_pack_unit_price,
            0                                                       as ap_tot_prime_cost_standard_co_pack,
    
            {{ dbt_utils.surrogate_key([
            "cast(ltrim(rtrim(upper(substring(stg.source_system,1,255)))) as text(255) ) ",
            "cast(ltrim(rtrim(upper(substring(plan_source_customer_code,1,255)))) as text(255) )",
            "cast(ltrim(rtrim(substring(source_item_identifier,1,255))) as text(255) )",
            "cast(calendar_date as timestamp_ntz(9))",
            "cast(ltrim(rtrim(substring(scen_code,1,255))) as text(255) )",
            "cast(snapshot_date as timestamp_ntz(9))"
        ]) }}                                                               as unique_key
    from stage stg  
    left join scenario_master scen_lkp
    on stg.source_system=scen_lkp.source_system
    and stg.scen_idx=scen_lkp.scenario_id
    left join customer_planning cust_plan_lkp
    on stg.plan_source_customer_code=cust_plan_lkp.trade_type_code
    left join
    {{
        ent_dbt_package.lkp_uom("stg.item_guid","'KG'","'CA'","uom_kg_ca_lkp",)
	}}
    left join
    {{
        ent_dbt_package.lkp_uom("stg.item_guid","'CA'","'KG'","uom_ca_kg_lkp",)
	}}
    left join
    {{
        ent_dbt_package.lkp_uom("stg.item_guid","'CA'","'PL'","uom_ca_pl_lkp",)
	}}
),
cbom_tfm as (
    select fc.* 
        ,first_value(cbom.raw_materials) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) as raw_material_unit_price1
        ,first_value(cbom.raw_materials) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) * fc.tot_vol_ca as ap_tot_prime_cost_standard_raw1
        ,first_value(cbom.packaging) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) as packaging_unit_price1
        ,first_value(cbom.packaging) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) * fc.tot_vol_ca as ap_tot_prime_cost_standard_packaging1
        ,first_value(cbom.labour) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) as labour_unit_price1
        ,first_value(cbom.labour) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) * fc.tot_vol_ca as ap_tot_prime_cost_standard_labour1
        ,first_value(cbom.bought_in) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) as bought_in_unit_price1
        ,first_value(cbom.bought_in) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) * fc.tot_vol_ca as ap_tot_prime_cost_standard_bought_in1
        ,first_value(cbom.other) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) as other_unit_price1
        ,first_value(cbom.other) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) * fc.tot_vol_ca as ap_tot_prime_cost_standard_other1
        ,first_value(cbom.co_pack) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) as co_pack_unit_price1
        ,first_value(cbom.co_pack) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) * fc.tot_vol_ca as ap_tot_prime_cost_standard_co_pack1
        ,first_value(cbom.gl_unit_price) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) as gl_unit_price1
        ,first_value(cbom.gl_unit_price) over (partition by upper(cbom.root_company_code),upper(cbom.root_src_item_identifier) order by cbom.eff_date desc) * fc.tot_vol_ca as ap_tot_prime_cost_standard1
        ,row_number() over (partition by unique_key  order by 1) rownum
from uom_conversion fc
    left outer join cbom cbom on upper(cbom.root_company_code) = 'WBX' and upper(fc.source_item_identifier) = upper(cbom.root_src_item_identifier) 
    and cbom.eff_date <= fc.calendar_date
),
cbom_merge as (
    select
        src.source_system as source_system,
        case when tgt.sku_idx=src.sku_idx then tgt.sku_idx else src.sku_idx end as sku_idx,
        src.source_item_identifier as source_item_identifier,
        case when tgt.item_guid=src.item_guid then tgt.item_guid else src.item_guid end as item_guid,
        case when tgt.cust_idx=src.cust_idx then tgt.cust_idx else src.cust_idx end as cust_idx,
        src.plan_source_customer_code as plan_source_customer_code,
        case when tgt.customer_address_number_guid=tgt.customer_address_number_guid then src.customer_address_number_guid
        else src.customer_address_number_guid end customer_address_number_guid,
        src.snapshot_date as snapshot_date,
        src.calendar_date as calendar_date,
        case when tgt.scen_idx=src.scen_idx then tgt.scen_idx else src.scen_idx end as scen_idx,
        src.scen_code as scen_code,
        case when tgt.scen_name=src.scen_name then tgt.scen_name else src.scen_name end as scen_name,
        case when tgt.scenario_guid=src.scenario_guid then tgt.scenario_guid else src.scenario_guid end as scenario_guid,
        case when tgt.isonpromo_si=src.isonpromo_si then tgt.isonpromo_si else src.isonpromo_si end as isonpromo_si,
        case when tgt.isonpromo_so=src.isonpromo_so then tgt.isonpromo_so else src.isonpromo_so end as isonpromo_so,
        case when tgt.tot_vol_sp_base_uom=src.tot_vol_sp_base_uom then tgt.tot_vol_sp_base_uom 
        else src.tot_vol_sp_base_uom end as tot_vol_sp_base_uom,
        case when tgt.tot_vol_sp_kg_uom=src.tot_vol_sp_kg_uom then tgt.tot_vol_sp_kg_uom 
        else src.tot_vol_sp_kg_uom end as tot_vol_sp_kg_uom,
        case when tgt.tot_vol_sp_ul_uom=src.tot_vol_sp_ul_uom then tgt.tot_vol_sp_ul_uom
        else src.tot_vol_sp_ul_uom end as tot_vol_sp_ul_uom,
        case when tgt.tot_vol_sp_base_uom_pre_adjustment=src.tot_vol_sp_base_uom_pre_adjustment then tgt.tot_vol_sp_base_uom_pre_adjustment
        else src.tot_vol_sp_base_uom_pre_adjustment end as tot_vol_sp_base_uom_pre_adjustment,
        case when tgt.tot_vol_sp_kg_uom_pre_adjustment=src.tot_vol_sp_kg_uom_pre_adjustment then tgt.tot_vol_sp_kg_uom_pre_adjustment
        else src.tot_vol_sp_kg_uom_pre_adjustment end as tot_vol_sp_kg_uom_pre_adjustment,
        case when tgt.tot_vol_sp_ul_uom_pre_adjustment=src.tot_vol_sp_ul_uom_pre_adjustment then tgt.tot_vol_sp_ul_uom_pre_adjustment
        else src.tot_vol_sp_ul_uom_pre_adjustment end as tot_vol_sp_ul_uom_pre_adjustment,
        case when tgt.tot_vol_sp_base_uom_mgmt_adjustment=src.tot_vol_sp_base_uom_mgmt_adjustment then tgt.tot_vol_sp_base_uom_mgmt_adjustment
        else src.tot_vol_sp_base_uom_mgmt_adjustment end as tot_vol_sp_base_uom_mgmt_adjustment,
        case when tgt.tot_vol_sp_kg_uom_mgmt_adjustment=src.tot_vol_sp_kg_uom_mgmt_adjustment then tgt.tot_vol_sp_kg_uom_mgmt_adjustment
        else src.tot_vol_sp_kg_uom_mgmt_adjustment end as tot_vol_sp_kg_uom_mgmt_adjustment,
        case when tgt.tot_vol_sp_ul_uom_mgmt_adjustment=src.tot_vol_sp_ul_uom_mgmt_adjustment then tgt.tot_vol_sp_ul_uom_mgmt_adjustment
        else src.tot_vol_sp_ul_uom_mgmt_adjustment end as tot_vol_sp_ul_uom_mgmt_adjustment,
        case when tgt.retail_tot_vol_sp_base_uom=src.retail_tot_vol_sp_base_uom then tgt.retail_tot_vol_sp_base_uom
        else src.retail_tot_vol_sp_base_uom end as retail_tot_vol_sp_base_uom,
        case when tgt.retail_tot_vol_sp_kg_uom=src.retail_tot_vol_sp_kg_uom then tgt.retail_tot_vol_sp_kg_uom 
        else src.retail_tot_vol_sp_kg_uom end as retail_tot_vol_sp_kg_uom,
        case when tgt.retail_tot_vol_sp_ul_uom=src.retail_tot_vol_sp_ul_uom then tgt.retail_tot_vol_sp_ul_uom 
        else src.retail_tot_vol_sp_ul_uom end as retail_tot_vol_sp_ul_uom ,
        case when tgt.promo_vol=src.promo_vol then tgt.promo_vol else src.promo_vol end as promo_vol,
        case when tgt.promo_vol_kg=src.promo_vol_kg then tgt.promo_vol_kg else src.promo_vol_kg end as promo_vol_kg,
        case when tgt.promo_vol_ul=src.promo_vol_ul then tgt.promo_vol_ul else src.promo_vol_ul end as promo_vol_ul,
        case when tgt.tot_vol_sgl=src.tot_vol_sgl then tgt.tot_vol_sgl else src.tot_vol_sgl end as tot_vol_sgl,
        case when tgt.tot_vol_sgl_ca=src.tot_vol_sgl_ca then tgt.tot_vol_sgl_ca else src.tot_vol_sgl_ca end as tot_vol_sgl_ca,
        case when tgt.tot_vol_sgl_ul=src.tot_vol_sgl_ul then tgt.tot_vol_sgl_ul else src.tot_vol_sgl_ul end as tot_vol_sgl_ul,
        case when tgt.tot_vol_kg=src.tot_vol_kg then tgt.tot_vol_kg else src.tot_vol_kg end as tot_vol_kg,
        case when tgt.tot_vol_ca=src.tot_vol_ca then tgt.tot_vol_ca else src.tot_vol_ca end as tot_vol_ca,
        case when tgt.tot_vol_ul=src.tot_vol_ul then tgt.tot_vol_ul else src.tot_vol_ul end as tot_vol_ul,
        case when tgt.ap_added_value_pack=src.ap_added_value_pack then tgt.ap_added_value_pack 
        else src.ap_added_value_pack end as ap_added_value_pack,
        case when tgt.ap_permanent_disc=src.ap_permanent_disc then tgt.ap_permanent_disc 
        else src.ap_permanent_disc end as ap_permanent_disc,
        case when tgt.ap_invoiced_sales_value=src.ap_invoiced_sales_value then tgt.ap_invoiced_sales_value
        else src.ap_invoiced_sales_value end as ap_invoiced_sales_value,
        case when tgt.ap_net_sales_value=src.ap_net_sales_value then tgt.ap_net_sales_value 
        else src.ap_net_sales_value end as ap_net_sales_value,
        case when tgt.ap_net_realisable_revenue=src.ap_net_realisable_revenue then tgt.ap_net_realisable_revenue
        else src.ap_net_realisable_revenue end as ap_net_realisable_revenue,
        case when tgt.ap_variable_trade=src.ap_variable_trade then tgt.ap_variable_trade 
        else src.ap_variable_trade end as ap_variable_trade,
        case when tgt.ap_net_net_sales_value=src.ap_net_net_sales_value then tgt.ap_net_net_sales_value 
        else src.ap_net_net_sales_value end as ap_net_net_sales_value,
        case when tgt.ap_fixed_trade_cust_invoiced=src.ap_fixed_trade_cust_invoiced then src.ap_fixed_trade_cust_invoiced
        else tgt.ap_fixed_trade_cust_invoiced end as ap_fixed_trade_cust_invoiced,
        case when tgt.ap_total_trade_cust_invoiced=src.ap_total_trade_cust_invoiced then tgt.ap_total_trade_cust_invoiced
        else src.ap_total_trade_cust_invoiced end as ap_total_trade_cust_invoiced,
        case when tgt.ap_fixed_trade_non_cust_invoiced=src.ap_fixed_trade_non_cust_invoiced then tgt.ap_fixed_trade_non_cust_invoiced 
        else src.ap_fixed_trade_non_cust_invoiced end as ap_fixed_trade_non_cust_invoiced,
        case when tgt.ap_total_trade=src.ap_total_trade then tgt.ap_total_trade else src.ap_total_trade end as ap_total_trade,
        case when tgt.ap_gross_margin_standard=src.ap_gross_margin_standard then tgt.ap_gross_margin_standard 
        else src.ap_gross_margin_standard end as ap_gross_margin_standard,
        case when tgt.ap_gross_margin_actual=src.ap_gross_margin_actual then tgt.ap_gross_margin_actual 
        else src.ap_gross_margin_actual end as ap_gross_margin_actual,
        case when tgt.ap_gcat_standard=src.ap_gcat_standard then tgt.ap_gcat_standard else src.ap_gcat_standard
        end as ap_gcat_standard,
        case when tgt.ap_gcat_actuals=src.ap_gcat_actuals then tgt.ap_gcat_actuals else src.ap_gcat_actuals 
        end as ap_gcat_actuals,
        case when tgt.ap_fixed_annual_payments_pre_adjustment=src.ap_fixed_annual_payments_pre_adjustment then 
        tgt.ap_fixed_annual_payments_pre_adjustment else src.ap_fixed_annual_payments_pre_adjustment
        end as ap_fixed_annual_payments_pre_adjustment,
        case when tgt.ap_fixed_annual_payments_mgmt_adjustment=src.ap_fixed_annual_payments_mgmt_adjustment then
        tgt.ap_fixed_annual_payments_mgmt_adjustment else src.ap_fixed_annual_payments_mgmt_adjustment
        end as ap_fixed_annual_payments_mgmt_adjustment,
        case when tgt.ap_fixed_annual_payments=src.ap_fixed_annual_payments then tgt.ap_fixed_annual_payments 
        else src.ap_fixed_annual_payments end as ap_fixed_annual_payments,
        case when tgt.ap_category_pre_adjustment=src.ap_category_pre_adjustment then tgt.ap_category_pre_adjustment
        else src.ap_category_pre_adjustment end as ap_category_pre_adjustment,
        case when tgt.ap_category_mgmt_adjustment=src.ap_category_mgmt_adjustment then tgt.ap_category_mgmt_adjustment
        else src.ap_category_mgmt_adjustment end as ap_category_mgmt_adjustment,
        case when tgt.ap_category=src.ap_category then tgt.ap_category else src.ap_category end as ap_category,
        case when tgt.ap_promo_fixed_funding_pre_adjustment=src.ap_promo_fixed_funding_pre_adjustment then
        tgt.ap_promo_fixed_funding_pre_adjustment else src.ap_promo_fixed_funding_pre_adjustment 
        end as ap_promo_fixed_funding_pre_adjustment,
        case when tgt.ap_promo_fixed_funding_mgmt_adjustment=src.ap_promo_fixed_funding_mgmt_adjustment then 
        tgt.ap_promo_fixed_funding_mgmt_adjustment else src.ap_promo_fixed_funding_mgmt_adjustment end as ap_promo_fixed_funding_mgmt_adjustment,
        case when tgt.ap_promo_fixed_funding=src.ap_promo_fixed_funding then tgt.ap_promo_fixed_funding 
        else src.ap_promo_fixed_funding end as ap_promo_fixed_funding,
        case when tgt.ap_cash_disc_pre_adjustment=src.ap_cash_disc_pre_adjustment then tgt.ap_cash_disc_pre_adjustment
        else src.ap_cash_disc_pre_adjustment end as ap_cash_disc_pre_adjustment,
        case when tgt.ap_cash_disc_mgmt_adjustment=src.ap_cash_disc_mgmt_adjustment then tgt.ap_cash_disc_mgmt_adjustment
        else src.ap_cash_disc_mgmt_adjustment end as ap_cash_disc_mgmt_adjustment,
        case when tgt.ap_cash_disc=src.ap_cash_disc then tgt.ap_cash_disc else src.ap_cash_disc end as ap_cash_disc,
        case when tgt.ap_direct_shopper_marketing_pre_adjustment=src.ap_direct_shopper_marketing_pre_adjustment
        then tgt.ap_direct_shopper_marketing_pre_adjustment else src.ap_direct_shopper_marketing_pre_adjustment
        end as ap_direct_shopper_marketing_pre_adjustment,
        case when tgt.ap_direct_shopper_marketing_mgmt_adjustment=src.ap_direct_shopper_marketing_mgmt_adjustment then
        tgt.ap_direct_shopper_marketing_mgmt_adjustment else src.ap_direct_shopper_marketing_mgmt_adjustment
        end as ap_direct_shopper_marketing_mgmt_adjustment,
        case when tgt.ap_direct_shopper_marketing=src.ap_direct_shopper_marketing then tgt.ap_direct_shopper_marketing
        else src.ap_direct_shopper_marketing end as ap_direct_shopper_marketing,
        case when tgt.ap_range_support_allowance_pre_adjustment=src.ap_range_support_allowance_pre_adjustment then
        tgt.ap_range_support_allowance_pre_adjustment else src.ap_range_support_allowance_pre_adjustment
        end as ap_range_support_allowance_pre_adjustment,
        case when tgt.ap_range_support_allowance_mgmt_adjustment=src.ap_range_support_allowance_mgmt_adjustment
        then tgt.ap_range_support_allowance_mgmt_adjustment else src.ap_range_support_allowance_mgmt_adjustment
        end as ap_range_support_allowance_mgmt_adjustment,
        case when tgt.ap_range_support_allowance=src.ap_range_support_allowance then tgt.ap_range_support_allowance
        else src.ap_range_support_allowance end as ap_range_support_allowance,
        case when tgt.ap_range_support_incentives_pre_adjustment=src.ap_range_support_incentives_pre_adjustment 
        then tgt.ap_range_support_incentives_pre_adjustment else src.ap_range_support_incentives_pre_adjustment
        end as ap_range_support_incentives_pre_adjustment,
        case when tgt.ap_range_support_incentives_mgmt_adjustment=src.ap_range_support_incentives_mgmt_adjustment
        then tgt.ap_range_support_incentives_mgmt_adjustment else src.ap_range_support_incentives_mgmt_adjustment 
        end as ap_range_support_incentives_mgmt_adjustment,
        case when tgt.ap_range_support_incentives=src.ap_range_support_incentives then tgt.ap_range_support_incentives
        else src.ap_range_support_incentives end as ap_range_support_incentives,
        case when tgt.ap_indirect_shopper_marketing_pre_adjustment=src.ap_indirect_shopper_marketing_pre_adjustment
        then tgt.ap_indirect_shopper_marketing_pre_adjustment else src.ap_indirect_shopper_marketing_pre_adjustment
        end as ap_indirect_shopper_marketing_pre_adjustment,
        case when tgt.ap_indirect_shopper_marketing_mgmt_adjustment=src.ap_indirect_shopper_marketing_mgmt_adjustment
        then tgt.ap_indirect_shopper_marketing_mgmt_adjustment else src.ap_indirect_shopper_marketing_mgmt_adjustment
        end as ap_indirect_shopper_marketing_mgmt_adjustment,
        case when tgt.ap_indirect_shopper_marketing=src.ap_indirect_shopper_marketing then tgt.ap_indirect_shopper_marketing
        else src.ap_indirect_shopper_marketing end as ap_indirect_shopper_marketing,
        case when tgt.ap_retro_pre_adjustment=src.ap_retro_pre_adjustment then tgt.ap_retro_pre_adjustment
        else src.ap_retro_pre_adjustment end as ap_retro_pre_adjustment,
        case when tgt.ap_retro_mgmt_adjustment=src.ap_retro_mgmt_adjustment then tgt.ap_retro_mgmt_adjustment
        else src.ap_retro_mgmt_adjustment end as ap_retro_mgmt_adjustment,
        case when tgt.ap_retro=src.ap_retro then tgt.ap_retro else src.ap_retro end as ap_retro,
        case when tgt.ap_avp_disc_pre_adjustment=src.ap_avp_disc_pre_adjustment then tgt.ap_avp_disc_pre_adjustment
        else src.ap_avp_disc_pre_adjustment end as ap_avp_disc_pre_adjustment,
        case when tgt.ap_avp_disc_mgmt_adjustment=src.ap_avp_disc_mgmt_adjustment then tgt.ap_avp_disc_mgmt_adjustment
        else src.ap_avp_disc_mgmt_adjustment end as ap_avp_disc_mgmt_adjustment,
        case when tgt.ap_avp_disc=src.ap_avp_disc then tgt.ap_avp_disc else src.ap_avp_disc end as ap_avp_disc,
        case when tgt.ap_everyday_low_prices_pre_adjustment=src.ap_everyday_low_prices_pre_adjustment then 
        tgt.ap_everyday_low_prices_pre_adjustment else src.ap_everyday_low_prices_pre_adjustment 
        end as ap_everyday_low_prices_pre_adjustment,
        case when tgt.ap_everyday_low_prices_mgmt_adjustment=src.ap_everyday_low_prices_mgmt_adjustment then
        tgt.ap_everyday_low_prices_mgmt_adjustment else src.ap_everyday_low_prices_mgmt_adjustment 
        end as ap_everyday_low_prices_mgmt_adjustment,
        case when tgt.ap_everyday_low_prices=src.ap_everyday_low_prices then tgt.ap_everyday_low_prices
        else src.ap_everyday_low_prices end as ap_everyday_low_prices,
        case when tgt.ap_off_invoice_disc_pre_adjustment=src.ap_off_invoice_disc_pre_adjustment then tgt.ap_off_invoice_disc_pre_adjustment
        else src.ap_off_invoice_disc_pre_adjustment end as ap_off_invoice_disc_pre_adjustment,
        case when tgt.ap_off_invoice_disc_mgmt_adjustment=src.ap_off_invoice_disc_mgmt_adjustment then 
        tgt.ap_off_invoice_disc_mgmt_adjustment else src.ap_off_invoice_disc_mgmt_adjustment 
        end as ap_off_invoice_disc_mgmt_adjustment,
        case when tgt.ap_off_invoice_disc=src.ap_off_invoice_disc then tgt.ap_off_invoice_disc 
        else src.ap_off_invoice_disc end as ap_off_invoice_disc,
        case when tgt.ap_field_marketing_pre_adjustment=src.ap_field_marketing_pre_adjustment then
        tgt.ap_field_marketing_pre_adjustment else src.ap_field_marketing_pre_adjustment
        end as ap_field_marketing_pre_adjustment,
        case when tgt.ap_field_marketing_mgmt_adjustment=src.ap_field_marketing_mgmt_adjustment then
        tgt.ap_field_marketing_mgmt_adjustment else src.ap_field_marketing_mgmt_adjustment 
        end as ap_field_marketing_mgmt_adjustment,
        case when tgt.ap_field_marketing=src.ap_field_marketing then tgt.ap_field_marketing 
        else src.ap_field_marketing end as ap_field_marketing,
        case when tgt.ap_tot_prime_cost_variance_pre_adjustment=src.ap_tot_prime_cost_variance_pre_adjustment
        then tgt.ap_tot_prime_cost_variance_pre_adjustment else src.ap_tot_prime_cost_variance_pre_adjustment
        end as ap_tot_prime_cost_variance_pre_adjustment,
        case when tgt.ap_tot_prime_cost_variance_mgmt_adjustment=src.ap_tot_prime_cost_variance_mgmt_adjustment
        then tgt.ap_tot_prime_cost_variance_mgmt_adjustment else src.ap_tot_prime_cost_variance_mgmt_adjustment
        end as ap_tot_prime_cost_variance_mgmt_adjustment,
        case when tgt.ap_tot_prime_cost_variance=src.ap_tot_prime_cost_variance then tgt.ap_tot_prime_cost_variance
        else src.ap_tot_prime_cost_variance end as ap_tot_prime_cost_variance,
        case when tgt.ap_tot_prime_cost_standard_pre_adjustment=src.ap_tot_prime_cost_standard_pre_adjustment
        then tgt.ap_tot_prime_cost_standard_pre_adjustment else src.ap_tot_prime_cost_standard_pre_adjustment
        end as ap_tot_prime_cost_standard_pre_adjustment,
        case when tgt.ap_tot_prime_cost_standard_mgmt_adjustment=src.ap_tot_prime_cost_standard_mgmt_adjustment 
        then tgt.ap_tot_prime_cost_standard_mgmt_adjustment else src.ap_tot_prime_cost_standard_mgmt_adjustment
        end as ap_tot_prime_cost_standard_mgmt_adjustment,
        case when tgt.ap_tot_prime_cost_standard=src.ap_tot_prime_cost_standard1 then tgt.ap_tot_prime_cost_standard
        else src.ap_tot_prime_cost_standard end as ap_tot_prime_cost_standard,
        case when tgt.ap_early_settlement_disc_pre_adjustment=src.ap_early_settlement_disc_pre_adjustment then
        tgt.ap_early_settlement_disc_pre_adjustment else src.ap_early_settlement_disc_pre_adjustment
        end as ap_early_settlement_disc_pre_adjustment,
        case when tgt.ap_early_settlement_disc_mgmt_adjustment=src.ap_early_settlement_disc_mgmt_adjustment then
        tgt.ap_early_settlement_disc_mgmt_adjustment else src.ap_early_settlement_disc_mgmt_adjustment
        end as ap_early_settlement_disc_mgmt_adjustment,
        case when tgt.ap_early_settlement_disc=src.ap_early_settlement_disc then tgt.ap_early_settlement_disc
        else src.ap_early_settlement_disc end as ap_early_settlement_disc,
        case when tgt.ap_other_direct_payments_pre_adjustment=src.ap_other_direct_payments_pre_adjustment then
        tgt.ap_other_direct_payments_pre_adjustment else src.ap_other_direct_payments_pre_adjustment
        end as ap_other_direct_payments_pre_adjustment,
        case when tgt.ap_other_direct_payments_mgmt_adjustment=src.ap_other_direct_payments_mgmt_adjustment then 
        tgt.ap_other_direct_payments_mgmt_adjustment else src.ap_other_direct_payments_mgmt_adjustment
        end as ap_other_direct_payments_mgmt_adjustment,
        case when tgt.ap_other_direct_payments=src.ap_other_direct_payments then tgt.ap_other_direct_payments
        else src.ap_other_direct_payments end as ap_other_direct_payments,
        case when tgt.ap_other_indirect_payments_pre_adjustment=src.ap_other_indirect_payments_pre_adjustment then
        tgt.ap_other_indirect_payments_pre_adjustment else src.ap_other_indirect_payments_pre_adjustment
        end as ap_other_indirect_payments_pre_adjustment,
        case when tgt.ap_other_indirect_payments_mgmt_adjustment=src.ap_other_indirect_payments_mgmt_adjustment then
        tgt.ap_other_indirect_payments_mgmt_adjustment else src.ap_other_indirect_payments_mgmt_adjustment
        end as ap_other_indirect_payments_mgmt_adjustment,
        case when tgt.ap_other_indirect_payments=src.ap_other_indirect_payments then tgt.ap_other_indirect_payments
        else src.ap_other_indirect_payments end as ap_other_indirect_payments,
        case when tgt.ap_gross_selling_value_pre_adjustment=src.ap_gross_selling_value_pre_adjustment then
        tgt.ap_gross_selling_value_pre_adjustment else src.ap_gross_selling_value_pre_adjustment
        end as ap_gross_selling_value_pre_adjustment,
        case when tgt.ap_gross_selling_value_mgmt_adjustment=src.ap_gross_selling_value_mgmt_adjustment then
        tgt.ap_gross_selling_value_mgmt_adjustment else src.ap_gross_selling_value_mgmt_adjustment
        end as ap_gross_selling_value_mgmt_adjustment,
        case when tgt.ap_gross_selling_value=src.ap_gross_selling_value then tgt.ap_gross_selling_value 
        else src.ap_gross_selling_value end as ap_gross_selling_value,
        case when tgt.ap_gross_sales_value=src.ap_gross_sales_value then tgt.ap_gross_sales_value
        else src.ap_gross_sales_value end as ap_gross_sales_value,
        case when tgt.ap_growth_incentives_pre_adjustment=src.ap_growth_incentives_pre_adjustment then
        tgt.ap_growth_incentives_pre_adjustment else src.ap_growth_incentives_pre_adjustment end as ap_growth_incentives_pre_adjustment,
        case when tgt.ap_growth_incentives_mgmt_adjustment=src.ap_growth_incentives_mgmt_adjustment then
        tgt.ap_growth_incentives_mgmt_adjustment else src.ap_growth_incentives_mgmt_adjustment end as ap_growth_incentives_mgmt_adjustment,
        case when tgt.ap_growth_incentives=src.ap_growth_incentives then tgt.ap_growth_incentives
        else src.ap_growth_incentives end as ap_growth_incentives,
        case when tgt.retail_tot_vol_sgl=src.retail_tot_vol_sgl then tgt.retail_tot_vol_sgl
        else src.retail_tot_vol_sgl end as retail_tot_vol_sgl,
        case when tgt.retail_tot_vol_sgl_ca=src.retail_tot_vol_sgl_ca then tgt.retail_tot_vol_sgl_ca
        else src.retail_tot_vol_sgl_ca end as retail_tot_vol_sgl_ca,
        case when tgt.retail_tot_vol_sgl_ul=src.retail_tot_vol_sgl_ul then tgt.retail_tot_vol_sgl_ul
        else src.retail_tot_vol_sgl_ul end as retail_tot_vol_sgl_ul,
        case when tgt.retail_tot_vol_kg=src.retail_tot_vol_kg then tgt.retail_tot_vol_kg
        else src.retail_tot_vol_kg end as retail_tot_vol_kg,
        case when tgt.retail_tot_vol_ca=src.retail_tot_vol_ca then tgt.retail_tot_vol_ca
        else src.retail_tot_vol_ca end as retail_tot_vol_ca,
        case when tgt.retail_tot_vol_ul=src.retail_tot_vol_ul then tgt.retail_tot_vol_ul
        else src.retail_tot_vol_ul end as retail_tot_vol_ul, 
        case when tgt.ap_retail_revenue_mrrsp=src.ap_retail_revenue_mrrsp then tgt.ap_retail_revenue_mrrsp
        else src.ap_retail_revenue_mrrsp end as ap_retail_revenue_mrrsp,
        case when tgt.ap_retail_revenue_rsp=src.ap_retail_revenue_rsp then tgt.ap_retail_revenue_rsp
        else src.ap_retail_revenue_rsp end as ap_retail_revenue_rsp,
        case when tgt.ap_retail_revenue_net=src.ap_retail_revenue_net then tgt.ap_retail_revenue_net
        else src.ap_retail_revenue_net end as ap_retail_revenue_net,
        case when tgt.ap_retail_cost_of_sales=src.ap_retail_cost_of_sales then tgt.ap_retail_cost_of_sales
        else src.ap_retail_cost_of_sales end as ap_retail_cost_of_sales,
        case when tgt.ap_retail_retailer_retro_funding=src.ap_retail_retailer_retro_funding then tgt.ap_retail_retailer_retro_funding
        else src.ap_retail_retailer_retro_funding end as ap_retail_retailer_retro_funding,
        case when tgt.ap_retail_margin_excl_fixed_funding=src.ap_retail_margin_excl_fixed_funding then 
        tgt.ap_retail_margin_excl_fixed_funding else src.ap_retail_margin_excl_fixed_funding end as ap_retail_margin_excl_fixed_funding,
        case when tgt.ap_retail_promo_fixed_spend=src.ap_retail_promo_fixed_spend then tgt.ap_retail_promo_fixed_spend
        else src.ap_retail_promo_fixed_spend end as ap_retail_promo_fixed_spend,
        case when tgt.ap_retail_total_spend=src.ap_retail_total_spend then tgt.ap_retail_total_spend
        else src.ap_retail_total_spend end as ap_retail_total_spend,
        case when tgt.ap_retail_margin_incl_fixed_funding=src.ap_retail_margin_incl_fixed_funding then
        tgt.ap_retail_margin_incl_fixed_funding else src.ap_retail_margin_incl_fixed_funding end as ap_retail_margin_incl_fixed_funding,
        case when tgt.ap_retail_revenue_net_excl_mrrsp=src.ap_retail_revenue_net_excl_mrrsp then tgt.ap_retail_revenue_net_excl_mrrsp
        else src.ap_retail_revenue_net_excl_mrrsp end as ap_retail_revenue_net_excl_mrrsp,
        case when tgt.ap_retail_revenue_net_excl_rsp=src.ap_retail_revenue_net_excl_rsp then tgt.ap_retail_revenue_net_excl_rsp
        else src.ap_retail_revenue_net_excl_rsp end as ap_retail_revenue_net_excl_rsp,
        case when tgt.gl_unit_price=src.gl_unit_price1 then tgt.gl_unit_price else src.gl_unit_price1 end as gl_unit_price,
        case when tgt.raw_material_unit_price=src.raw_material_unit_price1 then tgt.raw_material_unit_price
        else src.raw_material_unit_price1 end as raw_material_unit_price,
        case when tgt.ap_tot_prime_cost_standard_raw=src.ap_tot_prime_cost_standard_raw1 then tgt.ap_tot_prime_cost_standard_raw
        else src.ap_tot_prime_cost_standard_raw1 end as ap_tot_prime_cost_standard_raw,
        case when tgt.packaging_unit_price=src.packaging_unit_price1 then tgt.packaging_unit_price 
        else src.packaging_unit_price1 end as packaging_unit_price,
        case when tgt.ap_tot_prime_cost_standard_packaging=src.ap_tot_prime_cost_standard_packaging1 
        then tgt.ap_tot_prime_cost_standard_packaging else src.ap_tot_prime_cost_standard_packaging1
        end as ap_tot_prime_cost_standard_packaging,
        case when tgt.labour_unit_price=src.labour_unit_price1 then tgt.labour_unit_price
        else src.labour_unit_price1 end as labour_unit_price,
        case when tgt.ap_tot_prime_cost_standard_labour=src.ap_tot_prime_cost_standard_labour1 then tgt.ap_tot_prime_cost_standard_labour
        else src.ap_tot_prime_cost_standard_labour1 end as ap_tot_prime_cost_standard_labour , 
        case when tgt.bought_in_unit_price=src.bought_in_unit_price1 then tgt.bought_in_unit_price 
        else src.bought_in_unit_price1 end as bought_in_unit_price,
        case when tgt.ap_tot_prime_cost_standard_bought_in=src.ap_tot_prime_cost_standard_bought_in1
        then tgt.ap_tot_prime_cost_standard_bought_in else src.ap_tot_prime_cost_standard_bought_in1
        end as ap_tot_prime_cost_standard_bought_in,
        case when tgt.other_unit_price=src.other_unit_price1 then tgt.other_unit_price
        else src.other_unit_price1 end as other_unit_price,
        case when tgt.ap_tot_prime_cost_standard_other=src.ap_tot_prime_cost_standard_other1 then tgt.ap_tot_prime_cost_standard_other
        else src.ap_tot_prime_cost_standard_other1 end as ap_tot_prime_cost_standard_other,
        case when tgt.co_pack_unit_price=src.co_pack_unit_price1 then tgt.co_pack_unit_price
        else src.co_pack_unit_price1 end as co_pack_unit_price,
        case when tgt.ap_tot_prime_cost_standard_co_pack=src.ap_tot_prime_cost_standard_co_pack1 then tgt.ap_tot_prime_cost_standard_co_pack
        else src.ap_tot_prime_cost_standard_co_pack1 end as ap_tot_prime_cost_standard_co_pack,
        src.rownum as rownum,
        src.unique_key as unique_key
    from cbom_tfm src join  uom_conversion tgt
    on tgt.unique_key=src.unique_key
),
final as (
    select 
        cast(substring(source_system,1,255) as text(255) )                      as source_system  ,
        cast(sku_idx as number(38,0) )                                          as sku_idx ,
        cast(substring(source_item_identifier,1,255) as text(255) )             as source_item_identifier  ,
        cast(item_guid as text(255) )                                           as item_guid  ,
        cast(cust_idx as number(38,0) )                                         as cust_idx  ,
        cast(substring(plan_source_customer_code,1,255) as text(255) )          as plan_source_customer_code  ,
        cast(substring(customer_address_number_guid,1,255) as text(255) )       as customer_address_number_guid  ,
        cast(snapshot_date as date)                                             as snapshot_date  ,
        cast(calendar_date as timestamp_ntz(9) )                                as calendar_date  ,
        cast(scen_idx as number(38,0) )                                         as scen_idx  ,
        cast(substring(scen_code,1,255) as text(255) )                          as scen_code  ,
        cast(substring(scen_name,1,255) as text(255) )                          as scen_name  ,
        cast(scenario_guid as text(255) )                                       as scenario_guid  ,
        cast(substring(isonpromo_si,1,255) as text(255) )                       as isonpromo_si  ,
        cast(substring(isonpromo_so,1,255) as text(255) )                       as isonpromo_so  ,
        cast(tot_vol_sp_base_uom as float)                                      as tot_vol_sp_base_uom  ,
        cast(tot_vol_sp_kg_uom as float)                                        as tot_vol_sp_kg_uom  ,
        cast(tot_vol_sp_ul_uom as float)                                        as tot_vol_sp_ul_uom  ,
        cast(tot_vol_sp_base_uom_pre_adjustment as float)                       as tot_vol_sp_base_uom_pre_adjustment  ,
        cast(tot_vol_sp_kg_uom_pre_adjustment as float)                         as tot_vol_sp_kg_uom_pre_adjustment  ,
        cast(tot_vol_sp_ul_uom_pre_adjustment as float)                         as tot_vol_sp_ul_uom_pre_adjustment  ,
        cast(tot_vol_sp_base_uom_mgmt_adjustment as float)                      as tot_vol_sp_base_uom_mgmt_adjustment  ,
        cast(tot_vol_sp_kg_uom_mgmt_adjustment as float)                        as tot_vol_sp_kg_uom_mgmt_adjustment  ,
        cast(tot_vol_sp_ul_uom_mgmt_adjustment as float)                        as tot_vol_sp_ul_uom_mgmt_adjustment  ,
        cast(retail_tot_vol_sp_base_uom as float)                               as retail_tot_vol_sp_base_uom  ,
        cast(retail_tot_vol_sp_kg_uom as float)                                 as retail_tot_vol_sp_kg_uom  ,
        cast(retail_tot_vol_sp_ul_uom as float)                                 as retail_tot_vol_sp_ul_uom  ,
        cast(promo_vol as float)                                                as promo_vol  ,
        cast(promo_vol_kg as float)                                             as promo_vol_kg  ,
        cast(promo_vol_ul as float)                                             as promo_vol_ul  ,
        cast(tot_vol_sgl as float)                                              as tot_vol_sgl  ,
        cast(tot_vol_sgl_ca as float)                                           as tot_vol_sgl_ca  ,
        cast(tot_vol_sgl_ul as float)                                           as tot_vol_sgl_ul  ,
        cast(tot_vol_kg as float)                                               as tot_vol_kg  ,
        cast(tot_vol_ca as float)                                               as tot_vol_ca  ,
        cast(tot_vol_ul as float)                                               as tot_vol_ul  ,
        cast(ap_added_value_pack as float)                                      as ap_added_value_pack  ,
        cast(ap_permanent_disc as float)                                        as ap_permanent_disc  ,
        cast(ap_invoiced_sales_value as float)                                  as ap_invoiced_sales_value  ,
        cast(ap_net_sales_value as float)                                       as ap_net_sales_value  ,
        cast(ap_net_realisable_revenue as float)                                as ap_net_realisable_revenue  ,
        cast(ap_variable_trade as float)                                        as ap_variable_trade  ,
        cast(ap_net_net_sales_value as float)                                   as ap_net_net_sales_value  ,
        cast(ap_fixed_trade_cust_invoiced as float)                             as ap_fixed_trade_cust_invoiced  ,
        cast(ap_total_trade_cust_invoiced as float)                             as ap_total_trade_cust_invoiced  ,
        cast(ap_fixed_trade_non_cust_invoiced as float)                         as ap_fixed_trade_non_cust_invoiced  ,
        cast(ap_total_trade as float)                                           as ap_total_trade  ,
        cast(ap_gross_margin_standard as float)                                 as ap_gross_margin_standard  ,
        cast(ap_gross_margin_actual as float)                                   as ap_gross_margin_actual  ,
        cast(ap_gcat_standard as float)                                         as ap_gcat_standard  ,
        cast(ap_gcat_actuals as float)                                          as ap_gcat_actuals  ,
        cast(ap_fixed_annual_payments_pre_adjustment as float)                  as ap_fixed_annual_payments_pre_adjustment  ,
        cast(ap_fixed_annual_payments_mgmt_adjustment as float)                 as ap_fixed_annual_payments_mgmt_adjustment  ,
        cast(ap_fixed_annual_payments as float)                                 as ap_fixed_annual_payments  ,
        cast(ap_category_pre_adjustment as float)                               as ap_category_pre_adjustment  ,
        cast(ap_category_mgmt_adjustment as float)                              as ap_category_mgmt_adjustment  ,
        cast(ap_category as float)                                              as ap_category  ,
        cast(ap_promo_fixed_funding_pre_adjustment as float)                    as ap_promo_fixed_funding_pre_adjustment  ,
        cast(ap_promo_fixed_funding_mgmt_adjustment as float)                   as ap_promo_fixed_funding_mgmt_adjustment  ,
        cast(ap_promo_fixed_funding as float)                                   as ap_promo_fixed_funding  ,
        cast(ap_cash_disc_pre_adjustment as float)                              as ap_cash_disc_pre_adjustment  ,
        cast(ap_cash_disc_mgmt_adjustment as float)                             as ap_cash_disc_mgmt_adjustment  ,
        cast(ap_cash_disc as float)                                             as ap_cash_disc  ,
        cast(ap_direct_shopper_marketing_pre_adjustment as float)               as ap_direct_shopper_marketing_pre_adjustment  ,
        cast(ap_direct_shopper_marketing_mgmt_adjustment as float)              as ap_direct_shopper_marketing_mgmt_adjustment  ,
        cast(ap_direct_shopper_marketing as float)                              as ap_direct_shopper_marketing  ,
        cast(ap_range_support_allowance_pre_adjustment as float)                as ap_range_support_allowance_pre_adjustment  ,
        cast(ap_range_support_allowance_mgmt_adjustment as float)               as ap_range_support_allowance_mgmt_adjustment  ,
        cast(ap_range_support_allowance as float)                               as ap_range_support_allowance  ,
        cast(ap_range_support_incentives_pre_adjustment as float)               as ap_range_support_incentives_pre_adjustment  ,
        cast(ap_range_support_incentives_mgmt_adjustment as float)              as ap_range_support_incentives_mgmt_adjustment  ,
        cast(ap_range_support_incentives as float)                              as ap_range_support_incentives  ,
        cast(ap_indirect_shopper_marketing_pre_adjustment as float)             as ap_indirect_shopper_marketing_pre_adjustment  ,
        cast(ap_indirect_shopper_marketing_mgmt_adjustment as float)            as ap_indirect_shopper_marketing_mgmt_adjustment  ,
        cast(ap_indirect_shopper_marketing as float)                            as ap_indirect_shopper_marketing  ,
        cast(ap_retro_pre_adjustment as float)                                  as ap_retro_pre_adjustment  ,
        cast(ap_retro_mgmt_adjustment as float)                                 as ap_retro_mgmt_adjustment  ,
        cast(ap_retro as float)                                                 as ap_retro  ,
        cast(ap_avp_disc_pre_adjustment as float)                               as ap_avp_disc_pre_adjustment  ,
        cast(ap_avp_disc_mgmt_adjustment as float)                              as ap_avp_disc_mgmt_adjustment  ,
        cast(ap_avp_disc as float)                                              as ap_avp_disc  ,
        cast(ap_everyday_low_prices_pre_adjustment as float)                    as ap_everyday_low_prices_pre_adjustment  ,
        cast(ap_everyday_low_prices_mgmt_adjustment as float)                   as ap_everyday_low_prices_mgmt_adjustment  ,
        cast(ap_everyday_low_prices as float)                                   as ap_everyday_low_prices  ,
        cast(ap_off_invoice_disc_pre_adjustment as float)                       as ap_off_invoice_disc_pre_adjustment  ,
        cast(ap_off_invoice_disc_mgmt_adjustment as float)                      as ap_off_invoice_disc_mgmt_adjustment  ,
        cast(ap_off_invoice_disc as float)                                      as ap_off_invoice_disc  ,
        cast(ap_field_marketing_pre_adjustment as float)                        as ap_field_marketing_pre_adjustment  ,
        cast(ap_field_marketing_mgmt_adjustment as float)                       as ap_field_marketing_mgmt_adjustment  ,
        cast(ap_field_marketing as float)                                       as ap_field_marketing  ,
        cast(ap_tot_prime_cost_variance_pre_adjustment as float)                as ap_tot_prime_cost_variance_pre_adjustment  ,
        cast(ap_tot_prime_cost_variance_mgmt_adjustment as float)               as ap_tot_prime_cost_variance_mgmt_adjustment  ,
        cast(ap_tot_prime_cost_variance as float)                               as ap_tot_prime_cost_variance  ,
        cast(ap_tot_prime_cost_standard_pre_adjustment as float)                as ap_tot_prime_cost_standard_pre_adjustment  ,
        cast(ap_tot_prime_cost_standard_mgmt_adjustment as float)               as ap_tot_prime_cost_standard_mgmt_adjustment  ,
        cast(ap_tot_prime_cost_standard as float)                               as ap_tot_prime_cost_standard  ,
        cast(ap_early_settlement_disc_pre_adjustment as float)                  as ap_early_settlement_disc_pre_adjustment  ,
        cast(ap_early_settlement_disc_mgmt_adjustment as float)                 as ap_early_settlement_disc_mgmt_adjustment  ,
        cast(ap_early_settlement_disc as float)                                 as ap_early_settlement_disc  ,
        cast(ap_other_direct_payments_pre_adjustment as float)                  as ap_other_direct_payments_pre_adjustment  ,
        cast(ap_other_direct_payments_mgmt_adjustment as float)                 as ap_other_direct_payments_mgmt_adjustment  ,
        cast(ap_other_direct_payments as float)                                 as ap_other_direct_payments  ,
        cast(ap_other_indirect_payments_pre_adjustment as float)                as ap_other_indirect_payments_pre_adjustment  ,
        cast(ap_other_indirect_payments_mgmt_adjustment as float)               as ap_other_indirect_payments_mgmt_adjustment  ,
        cast(ap_other_indirect_payments as float)                               as ap_other_indirect_payments  ,
        cast(ap_gross_selling_value_pre_adjustment as float)                    as ap_gross_selling_value_pre_adjustment  ,
        cast(ap_gross_selling_value_mgmt_adjustment as float)                   as ap_gross_selling_value_mgmt_adjustment  ,
        cast(ap_gross_selling_value as float)                                   as ap_gross_selling_value  ,
        cast(ap_gross_sales_value as float)                                     as ap_gross_sales_value  ,
        cast(ap_growth_incentives_pre_adjustment as float)                      as ap_growth_incentives_pre_adjustment  ,
        cast(ap_growth_incentives_mgmt_adjustment as float)                     as ap_growth_incentives_mgmt_adjustment  ,
        cast(ap_growth_incentives as float)                                     as ap_growth_incentives  ,
        cast(nvl(retail_tot_vol_sgl,0) as float)                                as retail_tot_vol_sgl  ,
        cast(nvl(retail_tot_vol_sgl_ca,0) as float)                             as retail_tot_vol_sgl_ca  ,
        cast(nvl(retail_tot_vol_sgl_ul,0) as float)                             as retail_tot_vol_sgl_ul  ,
        cast(nvl(retail_tot_vol_kg,0) as float)                                 as retail_tot_vol_kg  ,
        cast(nvl(retail_tot_vol_ca,0) as float)                                 as retail_tot_vol_ca  ,
        cast(nvl(retail_tot_vol_ul,0) as float)                                 as retail_tot_vol_ul  ,
        cast(ap_retail_revenue_mrrsp as float)                                  as ap_retail_revenue_mrrsp  ,
        cast(ap_retail_revenue_rsp as float)                                    as ap_retail_revenue_rsp  ,
        cast(ap_retail_revenue_net as float)                                    as ap_retail_revenue_net  ,
        cast(ap_retail_cost_of_sales as float)                                  as ap_retail_cost_of_sales  ,
        cast(ap_retail_retailer_retro_funding as float)                         as ap_retail_retailer_retro_funding  ,
        cast(ap_retail_margin_excl_fixed_funding as float)                      as ap_retail_margin_excl_fixed_funding  ,
        cast(ap_retail_promo_fixed_spend as float)                              as ap_retail_promo_fixed_spend  ,
        cast(ap_retail_total_spend as float)                                    as ap_retail_total_spend  ,
        cast(ap_retail_margin_incl_fixed_funding as float)                      as ap_retail_margin_incl_fixed_funding  ,
        cast(ap_retail_revenue_net_excl_mrrsp as float)                         as ap_retail_revenue_net_excl_mrrsp  ,
        cast(ap_retail_revenue_net_excl_rsp as float)                           as ap_retail_revenue_net_excl_rsp  ,
        cast(gl_unit_price as float)                                            as gl_unit_price  ,
        cast(nvl(raw_material_unit_price,0) as float)                           as raw_material_unit_price  ,
        cast(nvl(ap_tot_prime_cost_standard_raw,0) as float)                    as ap_tot_prime_cost_standard_raw  ,
        cast(nvl(packaging_unit_price,0) as float)                              as packaging_unit_price  ,
        cast(nvl(ap_tot_prime_cost_standard_packaging,0) as float)              as ap_tot_prime_cost_standard_packaging  ,
        cast(nvl(labour_unit_price,0) as float)                                 as labour_unit_price  ,
        cast(nvl(ap_tot_prime_cost_standard_labour,0) as float)                 as ap_tot_prime_cost_standard_labour  ,
        cast(nvl(bought_in_unit_price,0) as float)                              as bought_in_unit_price  ,
        cast(nvl(ap_tot_prime_cost_standard_bought_in,0) as float)              as ap_tot_prime_cost_standard_bought_in  ,
        cast(nvl(other_unit_price,0) as float)                                  as other_unit_price  ,
        cast(nvl(ap_tot_prime_cost_standard_other,0) as float)                  as ap_tot_prime_cost_standard_other  ,
        cast(nvl(co_pack_unit_price,0) as float)                                as co_pack_unit_price  ,
        cast(nvl(ap_tot_prime_cost_standard_co_pack,0) as float)                as ap_tot_prime_cost_standard_co_pack  ,
        rownum                                                                  as rownum,
        {{ dbt_utils.surrogate_key([
            "cast(ltrim(rtrim(upper(substring(source_system,1,255)))) as text(255) ) ",
            "cast(ltrim(rtrim(upper(substring(plan_source_customer_code,1,255)))) as text(255) )",
            "cast(ltrim(rtrim(substring(source_item_identifier,1,255))) as text(255) )",
            "cast(calendar_date as timestamp_ntz(9))",
            "cast(ltrim(rtrim(upper(substring(scen_code,1,255)))) as text(255) )",
            "cast(snapshot_date as timestamp_ntz(9))"
        ]) }}                                                                   as unique_key
from cbom_merge
)
select * from final
where (source_system,source_item_identifier) in (select source_system,source_item_identifier from item_master)
