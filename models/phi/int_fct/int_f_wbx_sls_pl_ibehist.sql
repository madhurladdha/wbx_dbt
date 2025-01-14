{{
    config(
        tags=["hist", "pl","pl_hist"],
        snowflake_warehouse=env_var("DBT_WBX_SF_WH")
    )
}}


with stg as(
    select * from {{ref('stg_f_wbx_sls_pl_ibehist')}}
),


itm_cte as(
    select 
    distinct 
    source_item_identifier,
    item_guid,
    primary_uom
    from {{ref('dim_wbx_item')}}
),

trade_cte as 
 (
    select distinct company_code, trade_type from {{ ref("dim_wbx_cust_planning") }} 
 ),

company_master as (
        select * from {{ ref('dim_wbx_company')}}
),


currency_exch_rate_dly_dim_oc as 
    (
        select
            curr_from_code as curr_from_code,
            curr_to_code as curr_to_code,
            curr_conv_rt as curr_conv_rt,
            eff_from_d as eff_from_d,
            source_system as source_system
        from {{ ref("dim_wbx_exchange_rate_dly_oc") }}

),

ref_effective_currency_dim as 
    (
        select distinct
            source_system as source_system,
            company_code as company_code,
            company_default_currency_code as company_default_currency_code,
            parent_currency_code as parent_currency_code,
            effective_date as effective_date,
            expiration_date as expiration_date
        from {{ ref("src_ref_effective_currency_dim") }}
    ),


 src as(
    select 
        stg.source_system,
        to_timestamp(last_day("DATE", 'MONTH')) as date,
        stg.trade_type,
        'EUR' as txn_currency,
        'CA' as transaction_uom,
        itm_cte.primary_uom,
        itm_cte.item_guid,
        trade_cte.company_code,
        sku,
        volume,
        gross_selling_value,
        added_value_pack,
        growth_incentives,
        edlp,
        early_settlement_discount,
        rsa_incentives,
        retro,
        avp_discount,
        off_invoice,
        promo_fixed_funding,
        fixed_annual_payments,
        direct_shopper_marketing,
        other_direct_payments,
        pcos_std_ingredients,
        pcos_std_packaging,
        pcos_std_labour,
        pcos_std_co_packing,
        pcos_std_bought_in,
        pcos_std_other,
        pcos_var_ingredients,
        pcos_var_packaging,
        pcos_var_labour,
        pcos_var_co_packing,
        pcos_var_bought_in,
        pcos_var_other,
        indirect_shopper_marketing,
        category,
        other_indirect_payments,
        field_marketing,
        other_trade,
        marketing_agency_fees,
        research,
        continuous_research,
        market_research,
        sponsorship,
        sales_promotions,
        pack_artwork_design,
        pos_materials,
        samples_issued,
        pr,
        advertising_tv,
        tv_advertising_production,
        press_advertising_consumer,
        press_advertising_production_consumer,
        radio_time,
        radio_time_production,
        website_marketing,
        poster_space,
        poster_production,
        digital_media,
        digital_media_production
 from stg
 inner join itm_cte on itm_cte.source_item_identifier=stg.sku 
 inner join trade_cte on stg.trade_type = trade_cte.trade_type
 ),

exp as(
 select 
        src. source_system,
        date,
        trade_type,
        sku,
        src.item_guid,
        primary_uom,
        transaction_uom,
        src.company_code,
        volume,
        gross_selling_value,
        added_value_pack,
        growth_incentives,
        edlp,
        early_settlement_discount,
        rsa_incentives,
        retro,
        avp_discount,
        off_invoice,
        promo_fixed_funding,
        fixed_annual_payments,
        direct_shopper_marketing,
        other_direct_payments,
        pcos_std_ingredients,
        pcos_std_packaging,
        pcos_std_labour,
        pcos_std_co_packing,
        pcos_std_bought_in,
        pcos_std_other,
        pcos_var_ingredients,
        pcos_var_packaging,
        pcos_var_labour,
        pcos_var_co_packing,
        pcos_var_bought_in,
        pcos_var_other,
        indirect_shopper_marketing,
        category,
        other_indirect_payments,
        field_marketing,
        other_trade,
        marketing_agency_fees,
        research,
        continuous_research,
        market_research,
        sponsorship,
        sales_promotions,
        pack_artwork_design,
        pos_materials,
        samples_issued,
        pr,
        advertising_tv,
        tv_advertising_production,
        press_advertising_consumer,
        press_advertising_production_consumer,
        radio_time,
        radio_time_production,
        website_marketing,
        poster_space,
        poster_production,
        digital_media,
        digital_media_production,
        coalesce(conversion_rate_lkp.conversion_rate,0) as uom_conversion_rt,
        nvl( to_char(txn_currency_lkp.normalized_value), to_char(src.txn_currency)) as transaction_currency,
        ref_effective_currency_dim.company_default_currency_code as base_currency,
        ref_effective_currency_dim.parent_currency_code as pcomp_currency,
        'USD' as phi_currency,
        case when ref_effective_currency_dim.company_default_currency_code = nvl(to_char(txn_currency_lkp.normalized_value),to_char(src.txn_currency)) then 1 else coalesce(txn_conv_rt_lkp.curr_conv_rt, 0) end as txn_conv_rt,
        '1' as base_conv_rt,
        case when ref_effective_currency_dim.company_default_currency_code = 'USD' then 1  else coalesce(phi_conv_rt_lkp.curr_conv_rt, 0) end as phi_conv_rt,
        case  when ref_effective_currency_dim.company_default_currency_code  = ref_effective_currency_dim.parent_currency_code then 1 else coalesce(pcomp_conv_rt_lkp.curr_conv_rt, 0) end as pcomp_conv_rt
     
        from src
        left join
            ref_effective_currency_dim
            on ref_effective_currency_dim.source_system = src.source_system
            and ref_effective_currency_dim.company_code = src.company_code
            and ref_effective_currency_dim.effective_date <= src.date
            and ref_effective_currency_dim.expiration_date >= src.date

    left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "ADDRESS_BOOK",
                    "SUPP_CURRENCY_CODE",
                    "UPPER(SRC.txn_currency)",
                    "txn_currency_LKP",
                )
            }}


        left join
            {{
                lkp_exchange_rate_daily_oc(
                    "SRC.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "NVL(TO_CHAR(txn_currency_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.txn_currency))",
                    "SRC.date",
                    "txn_conv_rt_LKP",
                )
            }}

        left join
                /*phi currency conversion-----as currency conversion table haven't been roll forwarding the conv rts.using this sub sql to pick the latest conv rt */
          (SELECT SOURCE_SYSTEM,CURR_FROM_CODE,CURR_TO_CODE,EFF_FROM_D,CURR_CONV_RT,
            ROW_NUMBER() over (partition by SOURCE_SYSTEM,CURR_FROM_CODE,CURR_TO_CODE order by CURR_FROM_CODE,CURR_TO_CODE,EFF_FROM_D desc ) AS ROW_NUM
            FROM {{ref("v_dim_exchange_rate_dly_oc")}} ) phi_conv_rt_LKP
             ON phi_conv_rt_LKP.SOURCE_SYSTEM = SRC.SOURCE_SYSTEM
            AND phi_conv_rt_LKP.CURR_FROM_CODE = ref_effective_currency_dim.company_default_currency_code
            AND phi_conv_rt_LKP.CURR_TO_CODE = 'USD'
            --AND phi_conv_rt_LKP.EFF_FROM_D = CASE WHEN ref_effective_currency_dim.company_default_currency_code='USD' THEN '1900-01-01' ELSE SRC.date END
            AND phi_conv_rt_LKP.ROW_NUM =1

        left join
            {{
                lkp_exchange_rate_daily_oc(
                    "SRC.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "ref_effective_currency_dim.parent_currency_code",
                    "SRC.date",
                    "pcomp_conv_rt_LKP",
                )
            }}
        left  join
              {{
                ent_dbt_package.lkp_uom("src.item_guid","src.transaction_uom","src.primary_uom","conversion_rate_lkp",)
              }}

           
     
),



final as
(
select
 source_system,
date,
Trade_Type,
sku,
item_guid,
company_code,
primary_uom,
transaction_uom,
transaction_currency,
base_currency,
phi_currency,
pcomp_currency,
uom_conversion_rt,
txn_conv_rt,
base_conv_rt,
phi_conv_rt,
pcomp_conv_rt,
volume,
volume*uom_conversion_rt as v_volume,
base_conv_rt * gross_selling_value as base_gross_selling_value,
phi_conv_rt * gross_selling_value as phi_gross_selling_value,
pcomp_conv_rt * gross_selling_value as pcomp_gross_selling_value,
txn_conv_rt * gross_selling_value as txn_gross_selling_value,
base_conv_rt * added_value_pack as base_added_value_pack,
phi_conv_rt * added_value_pack as phi_added_value_pack,
pcomp_conv_rt * added_value_pack as pcomp_added_value_pack,
txn_conv_rt * added_value_pack as txn_added_value_pack,
base_conv_rt * growth_incentives as base_growth_incentives,
phi_conv_rt * growth_incentives as phi_growth_incentives,
pcomp_conv_rt * growth_incentives as pcomp_growth_incentives,
txn_conv_rt * growth_incentives as txn_growth_incentives,
base_conv_rt * edlp as base_edlp,
phi_conv_rt * edlp as phi_edlp,
pcomp_conv_rt * edlp as pcomp_edlp,
txn_conv_rt * edlp as txn_edlp,
base_conv_rt * early_settlement_discount as base_early_settlement_discount,
phi_conv_rt * early_settlement_discount as phi_early_settlement_discount,
pcomp_conv_rt * early_settlement_discount as pcomp_early_settlement_discount,
txn_conv_rt * early_settlement_discount as txn_early_settlement_discount,
base_conv_rt * rsa_incentives as base_rsa_incentives,
phi_conv_rt * rsa_incentives as phi_rsa_incentives,
pcomp_conv_rt * rsa_incentives as pcomp_rsa_incentives,
txn_conv_rt * rsa_incentives as txn_rsa_incentives,
base_conv_rt * retro as base_retro,
phi_conv_rt * retro as phi_retro,
pcomp_conv_rt * retro as pcomp_retro,
txn_conv_rt * retro as txn_retro,
base_conv_rt * avp_discount as base_avp_discount,
phi_conv_rt * avp_discount as phi_avp_discount,
pcomp_conv_rt * avp_discount as pcomp_avp_discount,
txn_conv_rt * avp_discount as txn_avp_discount,
base_conv_rt * off_invoice as base_off_invoice,
phi_conv_rt * off_invoice as phi_off_invoice,
pcomp_conv_rt * off_invoice as pcomp_off_invoice,
txn_conv_rt * off_invoice as txn_off_invoice,
base_conv_rt * promo_fixed_funding as base_promo_fixed_funding,
phi_conv_rt * promo_fixed_funding as phi_promo_fixed_funding,
pcomp_conv_rt * promo_fixed_funding as pcomp_promo_fixed_funding,
txn_conv_rt * promo_fixed_funding as txn_promo_fixed_funding,
base_conv_rt * fixed_annual_payments as base_fixed_annual_payments,
phi_conv_rt * fixed_annual_payments as phi_fixed_annual_payments,
pcomp_conv_rt * fixed_annual_payments as pcomp_fixed_annual_payments,
txn_conv_rt * fixed_annual_payments as txn_fixed_annual_payments,
base_conv_rt * direct_shopper_marketing as base_direct_shopper_marketing,
phi_conv_rt * direct_shopper_marketing as phi_direct_shopper_marketing,
pcomp_conv_rt * direct_shopper_marketing as pcomp_direct_shopper_marketing,
txn_conv_rt * direct_shopper_marketing as txn_direct_shopper_marketing,
base_conv_rt * other_direct_payments as base_other_direct_payments,
phi_conv_rt * other_direct_payments as phi_other_direct_payments,
pcomp_conv_rt * other_direct_payments as pcomp_other_direct_payments,
txn_conv_rt * other_direct_payments as txn_other_direct_payments,
base_conv_rt * pcos_std_ingredients as base_pcos_std_ingredients,
phi_conv_rt * pcos_std_ingredients as phi_pcos_std_ingredients,
pcomp_conv_rt * pcos_std_ingredients as pcomp_pcos_std_ingredients,
txn_conv_rt * pcos_std_ingredients as txn_pcos_std_ingredients,
base_conv_rt * pcos_std_packaging as base_pcos_std_packaging,
phi_conv_rt * pcos_std_packaging as phi_pcos_std_packaging,
pcomp_conv_rt * pcos_std_packaging as pcomp_pcos_std_packaging,
txn_conv_rt * pcos_std_packaging as txn_pcos_std_packaging,
base_conv_rt * pcos_std_labour as base_pcos_std_labour,
phi_conv_rt * pcos_std_labour as phi_pcos_std_labour,
pcomp_conv_rt * pcos_std_labour as pcomp_pcos_std_labour,
txn_conv_rt * pcos_std_labour as txn_pcos_std_labour,
base_conv_rt * pcos_std_co_packing as base_pcos_std_co_packing,
phi_conv_rt * pcos_std_co_packing as phi_pcos_std_co_packing,
pcomp_conv_rt * pcos_std_co_packing as pcomp_pcos_std_co_packing,
txn_conv_rt * pcos_std_co_packing as txn_pcos_std_co_packing,
base_conv_rt * pcos_std_bought_in as base_pcos_std_bought_in,
phi_conv_rt * pcos_std_bought_in as phi_pcos_std_bought_in,
pcomp_conv_rt * pcos_std_bought_in as pcomp_pcos_std_bought_in,
txn_conv_rt * pcos_std_bought_in as txn_pcos_std_bought_in,
base_conv_rt * pcos_std_other as base_pcos_std_other,
phi_conv_rt * pcos_std_other as phi_pcos_std_other,
pcomp_conv_rt * pcos_std_other as pcomp_pcos_std_other,
txn_conv_rt * pcos_std_other as txn_pcos_std_other,
base_conv_rt * pcos_var_ingredients as base_pcos_var_ingredients,
phi_conv_rt * pcos_var_ingredients as phi_pcos_var_ingredients,
pcomp_conv_rt * pcos_var_ingredients as pcomp_pcos_var_ingredients,
txn_conv_rt * pcos_var_ingredients as txn_pcos_var_ingredients,
base_conv_rt * pcos_var_packaging as base_pcos_var_packaging,
phi_conv_rt * pcos_var_packaging as phi_pcos_var_packaging,
pcomp_conv_rt * pcos_var_packaging as pcomp_pcos_var_packaging,
txn_conv_rt * pcos_var_packaging as txn_pcos_var_packaging,
base_conv_rt * pcos_var_labour as base_pcos_var_labour,
phi_conv_rt * pcos_var_labour as phi_pcos_var_labour,
pcomp_conv_rt * pcos_var_labour as pcomp_pcos_var_labour,
txn_conv_rt * pcos_var_labour as txn_pcos_var_labour,
base_conv_rt * pcos_var_co_packing as base_pcos_var_co_packing,
phi_conv_rt * pcos_var_co_packing as phi_pcos_var_co_packing,
pcomp_conv_rt * pcos_var_co_packing as pcomp_pcos_var_co_packing,
txn_conv_rt * pcos_var_co_packing as txn_pcos_var_co_packing,
base_conv_rt * pcos_var_bought_in as base_pcos_var_bought_in,
phi_conv_rt * pcos_var_bought_in as phi_pcos_var_bought_in,
pcomp_conv_rt * pcos_var_bought_in as pcomp_pcos_var_bought_in,
txn_conv_rt * pcos_var_bought_in as txn_pcos_var_bought_in,
base_conv_rt * pcos_var_other as base_pcos_var_other,
phi_conv_rt * pcos_var_other as phi_pcos_var_other,
pcomp_conv_rt * pcos_var_other as pcomp_pcos_var_other,
txn_conv_rt * pcos_var_other as txn_pcos_var_other,
base_conv_rt * indirect_shopper_marketing as base_indirect_shopper_marketing,
phi_conv_rt * indirect_shopper_marketing as phi_indirect_shopper_marketing,
pcomp_conv_rt * indirect_shopper_marketing as pcomp_indirect_shopper_marketing,
txn_conv_rt * indirect_shopper_marketing as txn_indirect_shopper_marketing,
base_conv_rt * category as base_category,
phi_conv_rt * category as phi_category,
pcomp_conv_rt * category as pcomp_category,
txn_conv_rt * category as txn_category,
base_conv_rt * other_indirect_payments as base_other_indirect_payments,
phi_conv_rt * other_indirect_payments as phi_other_indirect_payments,
pcomp_conv_rt * other_indirect_payments as pcomp_other_indirect_payments,
txn_conv_rt * other_indirect_payments as txn_other_indirect_payments,
base_conv_rt * field_marketing as base_field_marketing,
phi_conv_rt * field_marketing as phi_field_marketing,
pcomp_conv_rt * field_marketing as pcomp_field_marketing,
txn_conv_rt * field_marketing as txn_field_marketing,
base_conv_rt * other_trade as base_other_trade,
phi_conv_rt * other_trade as phi_other_trade,
pcomp_conv_rt * other_trade as pcomp_other_trade,
txn_conv_rt * other_trade as txn_other_trade,
base_conv_rt * marketing_agency_fees as base_marketing_agency_fees,
phi_conv_rt * marketing_agency_fees as phi_marketing_agency_fees,
pcomp_conv_rt * marketing_agency_fees as pcomp_marketing_agency_fees,
txn_conv_rt * marketing_agency_fees as txn_marketing_agency_fees,
base_conv_rt * research as base_research,
phi_conv_rt * research as phi_research,
pcomp_conv_rt * research as pcomp_research,
txn_conv_rt * research as txn_research,
base_conv_rt * continuous_research as base_continuous_research,
phi_conv_rt * continuous_research as phi_continuous_research,
pcomp_conv_rt * continuous_research as pcomp_continuous_research,
txn_conv_rt * continuous_research as txn_continuous_research,
base_conv_rt * market_research as base_market_research,
phi_conv_rt * market_research as phi_market_research,
pcomp_conv_rt * market_research as pcomp_market_research,
txn_conv_rt * market_research as txn_market_research,
base_conv_rt * sponsorship as base_sponsorship,
phi_conv_rt * sponsorship as phi_sponsorship,
pcomp_conv_rt * sponsorship as pcomp_sponsorship,
txn_conv_rt * sponsorship as txn_sponsorship,
base_conv_rt * sales_promotions as base_sales_promotions,
phi_conv_rt * sales_promotions as phi_sales_promotions,
pcomp_conv_rt * sales_promotions as pcomp_sales_promotions,
txn_conv_rt * sales_promotions as txn_sales_promotions,
base_conv_rt * pack_artwork_design as base_pack_artwork_design,
phi_conv_rt * pack_artwork_design as phi_pack_artwork_design,
pcomp_conv_rt * pack_artwork_design as pcomp_pack_artwork_design,
txn_conv_rt * pack_artwork_design as txn_pack_artwork_design,
base_conv_rt * pos_materials as base_pos_materials,
phi_conv_rt * pos_materials as phi_pos_materials,
pcomp_conv_rt * pos_materials as pcomp_pos_materials,
txn_conv_rt * pos_materials as txn_pos_materials,
base_conv_rt * samples_issued as base_samples_issued,
phi_conv_rt * samples_issued as phi_samples_issued,
pcomp_conv_rt * samples_issued as pcomp_samples_issued,
txn_conv_rt * samples_issued as txn_samples_issued,
base_conv_rt * pr as base_pr,
phi_conv_rt * pr as phi_pr,
pcomp_conv_rt * pr as pcomp_pr,
txn_conv_rt * pr as txn_pr,
base_conv_rt * advertising_tv as base_advertising_tv,
phi_conv_rt * advertising_tv as phi_advertising_tv,
pcomp_conv_rt * advertising_tv as pcomp_advertising_tv,
txn_conv_rt * advertising_tv as txn_advertising_tv,
base_conv_rt * tv_advertising_production as base_tv_advertising_production,
phi_conv_rt * tv_advertising_production as phi_tv_advertising_production,
pcomp_conv_rt * tv_advertising_production as pcomp_tv_advertising_production,
txn_conv_rt * tv_advertising_production as txn_tv_advertising_production,
base_conv_rt * press_advertising_consumer as base_press_advertising_consumer,
phi_conv_rt * press_advertising_consumer as phi_press_advertising_consumer,
pcomp_conv_rt * press_advertising_consumer as pcomp_press_advertising_consumer,
txn_conv_rt * press_advertising_consumer as txn_press_advertising_consumer,
base_conv_rt * press_advertising_production_consumer as base_press_advertising_production_consumer,
phi_conv_rt * press_advertising_production_consumer as phi_press_advertising_production_consumer,
pcomp_conv_rt * press_advertising_production_consumer as pcomp_press_advertising_production_consumer,
txn_conv_rt * press_advertising_production_consumer as txn_press_advertising_production_consumer,
base_conv_rt * radio_time as base_radio_time,
phi_conv_rt * radio_time as phi_radio_time,
pcomp_conv_rt * radio_time as pcomp_radio_time,
txn_conv_rt * radio_time as txn_radio_time,
base_conv_rt * radio_time_production as base_radio_time_production,
phi_conv_rt * radio_time_production as phi_radio_time_production,
pcomp_conv_rt * radio_time_production as pcomp_radio_time_production,
txn_conv_rt * radio_time_production as txn_radio_time_production,
base_conv_rt * website_marketing as base_website_marketing,
phi_conv_rt * website_marketing as phi_website_marketing,
pcomp_conv_rt * website_marketing as pcomp_website_marketing,
txn_conv_rt * website_marketing as txn_website_marketing,
base_conv_rt * poster_space as base_poster_space,
phi_conv_rt * poster_space as phi_poster_space,
pcomp_conv_rt * poster_space as pcomp_poster_space,
txn_conv_rt * poster_space as txn_poster_space,
base_conv_rt * poster_production as base_poster_production,
phi_conv_rt * poster_production as phi_poster_production,
pcomp_conv_rt * poster_production as pcomp_poster_production,
txn_conv_rt * poster_production as txn_poster_production,
base_conv_rt * digital_media as base_digital_media,
phi_conv_rt * digital_media as phi_digital_media,
pcomp_conv_rt * digital_media as pcomp_digital_media,
txn_conv_rt * digital_media as txn_digital_media,
base_conv_rt * digital_media_production as base_digital_media_production,
phi_conv_rt * digital_media_production as phi_digital_media_production,
pcomp_conv_rt * digital_media_production as pcomp_digital_media_production,
txn_conv_rt * digital_media_production as txn_digital_media_production

from exp

)


select 
 {{ dbt_utils.surrogate_key(["source_system","date","Trade_Type","sku"]) }}      as unique_key,
cast(substring(source_system, 1, 255) as text(255)) as source_system,
cast(date as timestamp_ntz(9)) as date,
cast(substring(Trade_Type, 1, 255) as text(255)) as Trade_Type,
cast(substring(sku, 1, 255) as text(255)) as sku,
cast(substring(item_guid, 1, 255) as text(255)) as item_guid,
cast(substring(company_code, 1, 255) as text(255)) as company_code,
cast(primary_uom as text(255)) as primary_uom,
cast(transaction_uom as text(255)) as transaction_uom,
cast(substring(transaction_currency, 1, 255) as text(255)) as transaction_currency,
cast(substring(base_currency, 1, 255) as text(255)) as base_currency,
cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,
cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,
cast(uom_conversion_rt as  number(29, 9)) as uom_conversion_rt,
cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,
cast(base_conv_rt as number(29, 9)) as base_conv_rt,
cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
cast(v_volume as number(29, 9)) as volume,
cast(coalesce(base_gross_selling_value,0) as number(38,10)) as base_gross_selling_value,
cast(coalesce(phi_gross_selling_value,0) as number(38,10)) as phi_gross_selling_value,
cast(coalesce(pcomp_gross_selling_value,0) as number(38,10)) as pcomp_gross_selling_value,
cast(coalesce(txn_gross_selling_value,0) as number(38,10)) as txn_gross_selling_value,
cast(coalesce(base_added_value_pack,0) as number(38,10)) as base_added_value_pack,
cast(coalesce(phi_added_value_pack,0) as number(38,10)) as phi_added_value_pack,
cast(coalesce(pcomp_added_value_pack,0) as number(38,10)) as pcomp_added_value_pack,
cast(coalesce(txn_added_value_pack,0) as number(38,10)) as txn_added_value_pack,
cast(coalesce(base_growth_incentives,0) as number(38,10)) as base_growth_incentives,
cast(coalesce(phi_growth_incentives,0) as number(38,10)) as phi_growth_incentives,
cast(coalesce(pcomp_growth_incentives,0) as number(38,10)) as pcomp_growth_incentives,
cast(coalesce(txn_growth_incentives,0) as number(38,10)) as txn_growth_incentives,
cast(coalesce(base_edlp,0) as number(38,10)) as base_edlp,
cast(coalesce(phi_edlp,0) as number(38,10)) as phi_edlp,
cast(coalesce(pcomp_edlp,0) as number(38,10)) as pcomp_edlp,
cast(coalesce(txn_edlp,0) as number(38,10)) as txn_edlp,
cast(coalesce(base_early_settlement_discount,0) as number(38,10)) as base_early_settlement_discount,
cast(coalesce(phi_early_settlement_discount,0) as number(38,10)) as phi_early_settlement_discount,
cast(coalesce(pcomp_early_settlement_discount,0) as number(38,10)) as pcomp_early_settlement_discount,
cast(coalesce(txn_early_settlement_discount,0) as number(38,10)) as txn_early_settlement_discount,
cast(coalesce(base_rsa_incentives,0) as number(38,10)) as base_rsa_incentives,
cast(coalesce(phi_rsa_incentives,0) as number(38,10)) as phi_rsa_incentives,
cast(coalesce(pcomp_rsa_incentives,0) as number(38,10)) as pcomp_rsa_incentives,
cast(coalesce(txn_rsa_incentives,0) as number(38,10)) as txn_rsa_incentives,
cast(coalesce(base_retro,0) as number(38,10)) as base_retro,
cast(coalesce(phi_retro,0) as number(38,10)) as phi_retro,
cast(coalesce(pcomp_retro,0) as number(38,10)) as pcomp_retro,
cast(coalesce(txn_retro,0) as number(38,10)) as txn_retro,
cast(coalesce(base_avp_discount,0) as number(38,10)) as base_avp_discount,
cast(coalesce(phi_avp_discount,0) as number(38,10)) as phi_avp_discount,
cast(coalesce(pcomp_avp_discount,0) as number(38,10)) as pcomp_avp_discount,
cast(coalesce(txn_avp_discount,0) as number(38,10)) as txn_avp_discount,
cast(coalesce(base_off_invoice,0) as number(38,10)) as base_off_invoice,
cast(coalesce(phi_off_invoice,0) as number(38,10)) as phi_off_invoice,
cast(coalesce(pcomp_off_invoice,0) as number(38,10)) as pcomp_off_invoice,
cast(coalesce(txn_off_invoice,0) as number(38,10)) as txn_off_invoice,
cast(coalesce(base_promo_fixed_funding,0) as number(38,10)) as base_promo_fixed_funding,
cast(coalesce(phi_promo_fixed_funding,0) as number(38,10)) as phi_promo_fixed_funding,
cast(coalesce(pcomp_promo_fixed_funding,0) as number(38,10)) as pcomp_promo_fixed_funding,
cast(coalesce(txn_promo_fixed_funding,0) as number(38,10)) as txn_promo_fixed_funding,
cast(coalesce(base_fixed_annual_payments,0) as number(38,10)) as base_fixed_annual_payments,
cast(coalesce(phi_fixed_annual_payments,0) as number(38,10)) as phi_fixed_annual_payments,
cast(coalesce(pcomp_fixed_annual_payments,0) as number(38,10)) as pcomp_fixed_annual_payments,
cast(coalesce(txn_fixed_annual_payments,0) as number(38,10)) as txn_fixed_annual_payments,
cast(coalesce(base_direct_shopper_marketing,0) as number(38,10)) as base_direct_shopper_marketing,
cast(coalesce(phi_direct_shopper_marketing,0) as number(38,10)) as phi_direct_shopper_marketing,
cast(coalesce(pcomp_direct_shopper_marketing,0) as number(38,10)) as pcomp_direct_shopper_marketing,
cast(coalesce(txn_direct_shopper_marketing,0) as number(38,10)) as txn_direct_shopper_marketing,
cast(coalesce(base_other_direct_payments,0) as number(38,10)) as base_other_direct_payments,
cast(coalesce(phi_other_direct_payments,0) as number(38,10)) as phi_other_direct_payments,
cast(coalesce(pcomp_other_direct_payments,0) as number(38,10)) as pcomp_other_direct_payments,
cast(coalesce(txn_other_direct_payments,0) as number(38,10)) as txn_other_direct_payments,
cast(coalesce(base_pcos_std_ingredients,0) as number(38,10)) as base_pcos_std_ingredients,
cast(coalesce(phi_pcos_std_ingredients,0) as number(38,10)) as phi_pcos_std_ingredients,
cast(coalesce(pcomp_pcos_std_ingredients,0) as number(38,10)) as pcomp_pcos_std_ingredients,
cast(coalesce(txn_pcos_std_ingredients,0) as number(38,10)) as txn_pcos_std_ingredients,
cast(coalesce(base_pcos_std_packaging,0) as number(38,10)) as base_pcos_std_packaging,
cast(coalesce(phi_pcos_std_packaging,0) as number(38,10)) as phi_pcos_std_packaging,
cast(coalesce(pcomp_pcos_std_packaging,0) as number(38,10)) as pcomp_pcos_std_packaging,
cast(coalesce(txn_pcos_std_packaging,0) as number(38,10)) as txn_pcos_std_packaging,
cast(coalesce(base_pcos_std_labour,0) as number(38,10)) as base_pcos_std_labour,
cast(coalesce(phi_pcos_std_labour,0) as number(38,10)) as phi_pcos_std_labour,
cast(coalesce(pcomp_pcos_std_labour,0) as number(38,10)) as pcomp_pcos_std_labour,
cast(coalesce(txn_pcos_std_labour,0) as number(38,10)) as txn_pcos_std_labour,
cast(coalesce(base_pcos_std_co_packing,0) as number(38,10)) as base_pcos_std_co_packing,
cast(coalesce(phi_pcos_std_co_packing,0) as number(38,10)) as phi_pcos_std_co_packing,
cast(coalesce(pcomp_pcos_std_co_packing,0) as number(38,10)) as pcomp_pcos_std_co_packing,
cast(coalesce(txn_pcos_std_co_packing,0) as number(38,10)) as txn_pcos_std_co_packing,
cast(coalesce(base_pcos_std_bought_in,0) as number(38,10)) as base_pcos_std_bought_in,
cast(coalesce(phi_pcos_std_bought_in,0) as number(38,10)) as phi_pcos_std_bought_in,
cast(coalesce(pcomp_pcos_std_bought_in,0) as number(38,10)) as pcomp_pcos_std_bought_in,
cast(coalesce(txn_pcos_std_bought_in,0) as number(38,10)) as txn_pcos_std_bought_in,
cast(coalesce(base_pcos_std_other,0) as number(38,10)) as base_pcos_std_other,
cast(coalesce(phi_pcos_std_other,0) as number(38,10)) as phi_pcos_std_other,
cast(coalesce(pcomp_pcos_std_other,0) as number(38,10)) as pcomp_pcos_std_other,
cast(coalesce(txn_pcos_std_other,0) as number(38,10)) as txn_pcos_std_other,
cast(coalesce(base_pcos_var_ingredients,0) as number(38,10)) as base_pcos_var_ingredients,
cast(coalesce(phi_pcos_var_ingredients,0) as number(38,10)) as phi_pcos_var_ingredients,
cast(coalesce(pcomp_pcos_var_ingredients,0) as number(38,10)) as pcomp_pcos_var_ingredients,
cast(coalesce(txn_pcos_var_ingredients,0) as number(38,10)) as txn_pcos_var_ingredients,
cast(coalesce(base_pcos_var_packaging,0) as number(38,10)) as base_pcos_var_packaging,
cast(coalesce(phi_pcos_var_packaging,0) as number(38,10)) as phi_pcos_var_packaging,
cast(coalesce(pcomp_pcos_var_packaging,0) as number(38,10)) as pcomp_pcos_var_packaging,
cast(coalesce(txn_pcos_var_packaging,0) as number(38,10)) as txn_pcos_var_packaging,
cast(coalesce(base_pcos_var_labour,0) as number(38,10)) as base_pcos_var_labour,
cast(coalesce(phi_pcos_var_labour,0) as number(38,10)) as phi_pcos_var_labour,
cast(coalesce(pcomp_pcos_var_labour,0) as number(38,10)) as pcomp_pcos_var_labour,
cast(coalesce(txn_pcos_var_labour,0) as number(38,10)) as txn_pcos_var_labour,
cast(coalesce(base_pcos_var_co_packing,0) as number(38,10)) as base_pcos_var_co_packing,
cast(coalesce(phi_pcos_var_co_packing,0) as number(38,10)) as phi_pcos_var_co_packing,
cast(coalesce(pcomp_pcos_var_co_packing,0) as number(38,10)) as pcomp_pcos_var_co_packing,
cast(coalesce(txn_pcos_var_co_packing,0) as number(38,10)) as txn_pcos_var_co_packing,
cast(coalesce(base_pcos_var_bought_in,0) as number(38,10)) as base_pcos_var_bought_in,
cast(coalesce(phi_pcos_var_bought_in,0) as number(38,10)) as phi_pcos_var_bought_in,
cast(coalesce(pcomp_pcos_var_bought_in,0) as number(38,10)) as pcomp_pcos_var_bought_in,
cast(coalesce(txn_pcos_var_bought_in,0) as number(38,10)) as txn_pcos_var_bought_in,
cast(coalesce(base_pcos_var_other,0) as number(38,10)) as base_pcos_var_other,
cast(coalesce(phi_pcos_var_other,0) as number(38,10)) as phi_pcos_var_other,
cast(coalesce(pcomp_pcos_var_other,0) as number(38,10)) as pcomp_pcos_var_other,
cast(coalesce(txn_pcos_var_other,0) as number(38,10)) as txn_pcos_var_other,
cast(coalesce(base_indirect_shopper_marketing,0) as number(38,10)) as base_indirect_shopper_marketing,
cast(coalesce(phi_indirect_shopper_marketing,0) as number(38,10)) as phi_indirect_shopper_marketing,
cast(coalesce(pcomp_indirect_shopper_marketing,0) as number(38,10)) as pcomp_indirect_shopper_marketing,
cast(coalesce(txn_indirect_shopper_marketing,0) as number(38,10)) as txn_indirect_shopper_marketing,
cast(coalesce(base_category,0) as number(38,10)) as base_category,
cast(coalesce(phi_category,0) as number(38,10)) as phi_category,
cast(coalesce(pcomp_category,0) as number(38,10)) as pcomp_category,
cast(coalesce(txn_category,0) as number(38,10)) as txn_category,
cast(coalesce(base_other_indirect_payments,0) as number(38,10)) as base_other_indirect_payments,
cast(coalesce(phi_other_indirect_payments,0) as number(38,10)) as phi_other_indirect_payments,
cast(coalesce(pcomp_other_indirect_payments,0) as number(38,10)) as pcomp_other_indirect_payments,
cast(coalesce(txn_other_indirect_payments,0) as number(38,10)) as txn_other_indirect_payments,
cast(coalesce(base_field_marketing,0) as number(38,10)) as base_field_marketing,
cast(coalesce(phi_field_marketing,0) as number(38,10)) as phi_field_marketing,
cast(coalesce(pcomp_field_marketing,0) as number(38,10)) as pcomp_field_marketing,
cast(coalesce(txn_field_marketing,0) as number(38,10)) as txn_field_marketing,
cast(coalesce(base_other_trade,0) as number(38,10)) as base_other_trade,
cast(coalesce(phi_other_trade,0) as number(38,10)) as phi_other_trade,
cast(coalesce(pcomp_other_trade,0) as number(38,10)) as pcomp_other_trade,
cast(coalesce(txn_other_trade,0) as number(38,10)) as txn_other_trade,
cast(coalesce(base_marketing_agency_fees,0) as number(38,10)) as base_marketing_agency_fees,
cast(coalesce(phi_marketing_agency_fees,0) as number(38,10)) as phi_marketing_agency_fees,
cast(coalesce(pcomp_marketing_agency_fees,0) as number(38,10)) as pcomp_marketing_agency_fees,
cast(coalesce(txn_marketing_agency_fees,0) as number(38,10)) as txn_marketing_agency_fees,
cast(coalesce(base_research,0) as number(38,10)) as base_research,
cast(coalesce(phi_research,0) as number(38,10)) as phi_research,
cast(coalesce(pcomp_research,0) as number(38,10)) as pcomp_research,
cast(coalesce(txn_research,0) as number(38,10)) as txn_research,
cast(coalesce(base_continuous_research,0) as number(38,10)) as base_continuous_research,
cast(coalesce(phi_continuous_research,0) as number(38,10)) as phi_continuous_research,
cast(coalesce(pcomp_continuous_research,0) as number(38,10)) as pcomp_continuous_research,
cast(coalesce(txn_continuous_research,0) as number(38,10)) as txn_continuous_research,
cast(coalesce(base_market_research,0) as number(38,10)) as base_market_research,
cast(coalesce(phi_market_research,0) as number(38,10)) as phi_market_research,
cast(coalesce(pcomp_market_research,0) as number(38,10)) as pcomp_market_research,
cast(coalesce(txn_market_research,0) as number(38,10)) as txn_market_research,
cast(coalesce(base_sponsorship,0) as number(38,10)) as base_sponsorship,
cast(coalesce(phi_sponsorship,0) as number(38,10)) as phi_sponsorship,
cast(coalesce(pcomp_sponsorship,0) as number(38,10)) as pcomp_sponsorship,
cast(coalesce(txn_sponsorship,0) as number(38,10)) as txn_sponsorship,
cast(coalesce(base_sales_promotions,0) as number(38,10)) as base_sales_promotions,
cast(coalesce(phi_sales_promotions,0) as number(38,10)) as phi_sales_promotions,
cast(coalesce(pcomp_sales_promotions,0) as number(38,10)) as pcomp_sales_promotions,
cast(coalesce(txn_sales_promotions,0) as number(38,10)) as txn_sales_promotions,
cast(coalesce(base_pack_artwork_design,0) as number(38,10)) as base_pack_artwork_design,
cast(coalesce(phi_pack_artwork_design,0) as number(38,10)) as phi_pack_artwork_design,
cast(coalesce(pcomp_pack_artwork_design,0) as number(38,10)) as pcomp_pack_artwork_design,
cast(coalesce(txn_pack_artwork_design,0) as number(38,10)) as txn_pack_artwork_design,
cast(coalesce(base_pos_materials,0) as number(38,10)) as base_pos_materials,
cast(coalesce(phi_pos_materials,0) as number(38,10)) as phi_pos_materials,
cast(coalesce(pcomp_pos_materials,0) as number(38,10)) as pcomp_pos_materials,
cast(coalesce(txn_pos_materials,0) as number(38,10)) as txn_pos_materials,
cast(coalesce(base_samples_issued,0) as number(38,10)) as base_samples_issued,
cast(coalesce(phi_samples_issued,0) as number(38,10)) as phi_samples_issued,
cast(coalesce(pcomp_samples_issued,0) as number(38,10)) as pcomp_samples_issued,
cast(coalesce(txn_samples_issued,0) as number(38,10)) as txn_samples_issued,
cast(coalesce(base_pr,0) as number(38,10)) as base_pr,
cast(coalesce(phi_pr,0) as number(38,10)) as phi_pr,
cast(coalesce(pcomp_pr,0) as number(38,10)) as pcomp_pr,
cast(coalesce(txn_pr,0) as number(38,10)) as txn_pr,
cast(coalesce(base_advertising_tv,0) as number(38,10)) as base_advertising_tv,
cast(coalesce(phi_advertising_tv,0) as number(38,10)) as phi_advertising_tv,
cast(coalesce(pcomp_advertising_tv,0) as number(38,10)) as pcomp_advertising_tv,
cast(coalesce(txn_advertising_tv,0) as number(38,10)) as txn_advertising_tv,
cast(coalesce(base_tv_advertising_production,0) as number(38,10)) as base_tv_advertising_production,
cast(coalesce(phi_tv_advertising_production,0) as number(38,10)) as phi_tv_advertising_production,
cast(coalesce(pcomp_tv_advertising_production,0) as number(38,10)) as pcomp_tv_advertising_production,
cast(coalesce(txn_tv_advertising_production,0) as number(38,10)) as txn_tv_advertising_production,
cast(coalesce(base_press_advertising_consumer,0) as number(38,10)) as base_press_advertising_consumer,
cast(coalesce(phi_press_advertising_consumer,0) as number(38,10)) as phi_press_advertising_consumer,
cast(coalesce(pcomp_press_advertising_consumer,0) as number(38,10)) as pcomp_press_advertising_consumer,
cast(coalesce(txn_press_advertising_consumer,0) as number(38,10)) as txn_press_advertising_consumer,
cast(coalesce(base_press_advertising_production_consumer,0) as number(38,10)) as base_press_advertising_production_consumer,
cast(coalesce(phi_press_advertising_production_consumer,0) as number(38,10)) as phi_press_advertising_production_consumer,
cast(coalesce(pcomp_press_advertising_production_consumer,0) as number(38,10)) as pcomp_press_advertising_production_consumer,
cast(coalesce(txn_press_advertising_production_consumer,0) as number(38,10)) as txn_press_advertising_production_consumer,
cast(coalesce(base_radio_time,0) as number(38,10)) as base_radio_time,
cast(coalesce(phi_radio_time,0) as number(38,10)) as phi_radio_time,
cast(coalesce(pcomp_radio_time,0) as number(38,10)) as pcomp_radio_time,
cast(coalesce(txn_radio_time,0) as number(38,10)) as txn_radio_time,
cast(coalesce(base_radio_time_production,0) as number(38,10)) as base_radio_time_production,
cast(coalesce(phi_radio_time_production,0) as number(38,10)) as phi_radio_time_production,
cast(coalesce(pcomp_radio_time_production,0) as number(38,10)) as pcomp_radio_time_production,
cast(coalesce(txn_radio_time_production,0) as number(38,10)) as txn_radio_time_production,
cast(coalesce(base_website_marketing,0) as number(38,10)) as base_website_marketing,
cast(coalesce(phi_website_marketing,0) as number(38,10)) as phi_website_marketing,
cast(coalesce(pcomp_website_marketing,0) as number(38,10)) as pcomp_website_marketing,
cast(coalesce(txn_website_marketing,0) as number(38,10)) as txn_website_marketing,
cast(coalesce(base_poster_space,0) as number(38,10)) as base_poster_space,
cast(coalesce(phi_poster_space,0) as number(38,10)) as phi_poster_space,
cast(coalesce(pcomp_poster_space,0) as number(38,10)) as pcomp_poster_space,
cast(coalesce(txn_poster_space,0) as number(38,10)) as txn_poster_space,
cast(coalesce(base_poster_production,0) as number(38,10)) as base_poster_production,
cast(coalesce(phi_poster_production,0) as number(38,10)) as phi_poster_production,
cast(coalesce(pcomp_poster_production,0) as number(38,10)) as pcomp_poster_production,
cast(coalesce(txn_poster_production,0) as number(38,10)) as txn_poster_production,
cast(coalesce(base_digital_media,0) as number(38,10)) as base_digital_media,
cast(coalesce(phi_digital_media,0) as number(38,10)) as phi_digital_media,
cast(coalesce(pcomp_digital_media,0) as number(38,10)) as pcomp_digital_media,
cast(coalesce(txn_digital_media,0) as number(38,10)) as txn_digital_media,
cast(coalesce(base_digital_media_production,0) as number(38,10)) as base_digital_media_production,
cast(coalesce(phi_digital_media_production,0) as number(38,10)) as phi_digital_media_production,
cast(coalesce(pcomp_digital_media_production,0) as number(38,10)) as pcomp_digital_media_production,
cast(coalesce(txn_digital_media_production,0) as number(38,10)) as txn_digital_media_production

 from final






              