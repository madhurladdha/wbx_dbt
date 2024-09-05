{{
    config(
        tags=["ax_hist_fact","ax_hist_sales","ax_hist_on_demand"],
        on_schema_change="sync_all_columns",
    )
}}


with
    old_fct as (
        select * from {{ source("FACTS_FOR_COMPARE", "sls_wtx_fin_slsbudget_fact") }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
    ),
    converted_fct as (
        select
            cast(substring(source_system, 1, 10) as text(10)) as source_system,
            cast(
                substring(product_class_code, 1, 60) as text(60)
            ) as product_class_code,
            cast(
                substring(source_item_identifier, 1, 60) as text(60)
            ) as source_item_identifier,
            --cast(item_guid as text(255)) as item_guid,
            cast({{dbt_utils.surrogate_key(["source_system", "ltrim(rtrim(source_item_identifier))",])}} as text(255) ) as item_guid  ,
            cast(substring(trade_type_code, 1, 60) as text(60)) as trade_type_code,
            cast(
                substring(ship_source_customer_code, 1, 255) as text(255)
            ) as ship_source_customer_code,
            cast(ship_customer_address_guid as text(255)) as ship_customer_address_guid,
            cast(
                substring(bill_source_customer_code, 1, 255) as text(255)
            ) as bill_source_customer_code,
            cast(bill_customer_address_guid as text(255)) as bill_customer_address_guid,
            cast(calendar_date as timestamp_ntz(9)) as calendar_date,
            cast(substring(calendar_month, 1, 255) as text(255)) as calendar_month,
            cast(
                substring(fiscal_period_number, 1, 255) as text(255)
            ) as fiscal_period_number,
            cast(substring(frozen_forecast, 1, 10) as text(10)) as frozen_forecast,
            cast(budget_qty_ca as number(38, 10)) as budget_qty_ca,
            cast(budget_qty_kg as number(38, 10)) as budget_qty_kg,
            cast(budget_qty_ul as number(38, 10)) as budget_qty_ul,
            cast(budget_qty_prim as number(38, 10)) as budget_qty_prim,
            cast(substring(primary_uom, 1, 255) as text(255)) as primary_uom,
            cast(avp_qty_kg as number(38, 10)) as avp_qty_kg,
            cast(bars_qty_kg as number(38, 10)) as bars_qty_kg,
            cast(substring(currency_code, 1, 255) as text(255)) as currency_code,
            cast(waste_reduced_amt as number(38, 10)) as waste_reduced_amt,
            cast(cleaning_amt as number(38, 10)) as cleaning_amt,
            cast(engineer_amt as number(38, 10)) as engineer_amt,
            cast(labour_adj_amt as number(38, 10)) as labour_adj_amt,
            cast(gross_value_amt as number(38, 10)) as gross_value_amt,
            cast(edlp_amt as number(38, 10)) as edlp_amt,
            cast(rsa_amt as number(38, 10)) as rsa_amt,
            cast(settlement_amt as number(38, 10)) as settlement_amt,
            cast(gincent_amt as number(38, 10)) as gincent_amt,
            cast(incentive_forced_amt as number(38, 10)) as incentive_forced_amt,
            cast(incentive_addl_amt as number(38, 10)) as incentive_addl_amt,
            cast(other_amt as number(38, 10)) as other_amt,
            cast(back_margin_amt as number(38, 10)) as back_margin_amt,
            cast(net_value_amt as number(38, 10)) as net_value_amt,
            cast(avp_grossup_amt as number(38, 10)) as avp_grossup_amt,
            cast(net_value_grossup_amt as number(38, 10)) as net_value_grossup_amt,
            cast(rawmats_cost_amt as number(38, 10)) as rawmats_cost_amt,
            cast(pack_cost_amt as number(38, 10)) as pack_cost_amt,
            cast(labour_cost_amt as number(38, 10)) as labour_cost_amt,
            cast(boughtin_cost_amt as number(38, 10)) as boughtin_cost_amt,
            cast(copack_cost_amt as number(38, 10)) as copack_cost_amt,
            cast(rye_adj_cost_amt as number(38, 10)) as rye_adj_cost_amt,
            cast(total_cost_amt as number(38, 10)) as total_cost_amt,
            cast(exp_trade_spend_amt as number(38, 10)) as exp_trade_spend_amt,
            cast(exp_consumer_spend_amt as number(38, 10)) as exp_consumer_spend_amt,
            cast(pif_isa_amt as number(38, 10)) as pif_isa_amt,
            cast(pif_trade_amt as number(38, 10)) as pif_trade_amt,
            cast(pif_trade_oib_amt as number(38, 10)) as pif_trade_oib_amt,
            cast(pif_trade_red_amt as number(38, 10)) as pif_trade_red_amt,
            cast(pif_trade_avp_amt as number(38, 10)) as pif_trade_avp_amt,
            cast(pif_trade_enh_amt as number(38, 10)) as pif_trade_enh_amt,
            cast(mif_category_amt as number(38, 10)) as mif_category_amt,
            cast(mif_customer_mktg_amt as number(38, 10)) as mif_customer_mktg_amt,
            cast(mif_field_mktg_amt as number(38, 10)) as mif_field_mktg_amt,
            cast(mif_isa_amt as number(38, 10)) as mif_isa_amt,
            cast(
                mif_range_support_incent_amt as number(38, 10)
            ) as mif_range_support_incent_amt,
            cast(mif_trade_amt as number(38, 10)) as mif_trade_amt,
            cast(is_extra_amt as number(38, 10)) as is_extra_amt,
            cast(load_date as timestamp_ntz(9)) as load_date,
            cast(update_date as timestamp_ntz(9)) as update_date,
            {{
                dbt_utils.surrogate_key(
                    [
                        "cast(ltrim(rtrim(upper(substring(source_system,1,255)))) as text(255))",
                        "cast(ltrim(rtrim(substring(source_item_identifier,1,255))) as text(255))",
                        "cast(ltrim(rtrim(substring(FROZEN_FORECAST,1,255))) as text(255))",
                        "cast(ltrim(rtrim(substring(TRADE_TYPE_CODE,1,255))) as text(255))",
                        "cast(calendar_date as timestamp_ntz(9))",
                    ]
                )
            }}
            as unique_key
        from old_fct
    )

select *
from converted_fct
