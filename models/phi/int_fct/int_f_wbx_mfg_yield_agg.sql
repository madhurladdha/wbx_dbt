{{ config(tags=["wbx", "manufacturing", "yield", "agg"]) }}

with
    mfg_wtx_yield_inter_fact as (select * from {{ ref("fct_wbx_mfg_yield_inter") }}),
    max_variant_code as (
        select work_order_number, max(y2.wo_src_variant_code) wo_src_variant_code
        from mfg_wtx_yield_inter_fact y2
        group by work_order_number
    ),
    source as (
        select
            flag,
            y.work_order_number,
            comp_src_item_identifier,
            comp_src_variant_code,
            sum(actual_transaction_qty) actual_transaction_qty,
            max(transaction_date) transaction_date,
            comp_item_type,
            source_bom_identifier,
            wo_src_item_identifier,
            m.wo_src_variant_code,
            source_business_unit_code,
            max(comp_standard_quantity) comp_standard_qty,
            max(comp_perfection_quantity) comp_perfection_qty,
            max(comp_scrap_percent) comp_scrap_percent,
            comp_stock_site,
            financial_site,
            wo_stock_site,
            voucher,
            company_code,
            comp_transaction_uom,
            transaction_currency,
            item_match_bom_flag,
            sum(transaction_amt) transaction_amt,
            sum(stock_adj_qty) stock_adj_qty,
            product_class,
            comp_item_model_group,
            wo_item_model_group,
            bulk_flag,
            consolidated_batch_order,
            sum(trandt_actual_amount) trandt_actual_amount,
            sum(gldt_actual_amount) gldt_actual_amount,
            max(standard_amount) standard_amount,
            max(perfection_amount) perfection_amount,
            max(gldt_unit_price) gldt_unit_price,
            max(gl_date) as gl_date
        from mfg_wtx_yield_inter_fact y
        left join max_variant_code m on y.work_order_number = m.work_order_number
        group by
            flag,
            y.work_order_number,
            comp_src_item_identifier,
            comp_src_variant_code,
            comp_item_type,
            source_bom_identifier,
            wo_src_item_identifier,
            m.wo_src_variant_code,
            source_business_unit_code,
            comp_stock_site,
            financial_site,
            wo_stock_site,
            voucher,
            company_code,
            transaction_currency,
            m.wo_src_variant_code,
            item_match_bom_flag,
            product_class,
            bulk_flag,
            consolidated_batch_order,
            comp_transaction_uom,
            comp_item_model_group,
            wo_item_model_group
    ),
    final as (
        select
            *,
            current_timestamp as load_date,
            current_timestamp as update_date,
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            case
                when stock_adj_qty * gldt_unit_price is null
                then 0
                else stock_adj_qty * gldt_unit_price
            end as gldt_stock_adj_amount
        from source
    ),
    final_with_casting as (
        select
            cast(substring(source_system, 1, 255) as text(255)) as source_system,

            cast(substring(comp_stock_site, 1, 255) as text(255)) as comp_stock_site,

            cast(substring(financial_site, 1, 255) as text(255)) as financial_site,

            cast(substring(voucher, 1, 255) as text(255)) as voucher,

            cast(
                substring(work_order_number, 1, 255) as text(255)
            ) as work_order_number,

            cast(
                substring(comp_src_item_identifier, 1, 255) as text(255)
            ) as comp_src_item_identifier,

            cast(
                substring(comp_src_variant_code, 1, 255) as text(255)
            ) as comp_src_variant_code,

            cast(transaction_date as timestamp_ntz(9)) as transaction_date,

            cast(substring(comp_item_type, 1, 255) as text(255)) as comp_item_type,

            cast(
                substring(source_bom_identifier, 1, 255) as text(255)
            ) as source_bom_identifier,

            cast(
                substring(wo_src_item_identifier, 1, 255) as text(255)
            ) as wo_src_item_identifier,
            
            cast(
                substring(wo_src_variant_code, 1, 255) as text(255)
            ) as wo_src_variant_code,

            cast(
                substring(source_business_unit_code, 1, 255) as text(255)
            ) as source_business_unit_code,

            cast(substring(company_code, 1, 255) as text(255)) as company_code,

            cast(
                substring(comp_transaction_uom, 1, 255) as text(255)
            ) as comp_transaction_uom,

            cast(
                substring(transaction_currency, 1, 255) as text(255)
            ) as transaction_currency,

            cast(actual_transaction_qty as number(38, 10)) as actual_transaction_qty,

            cast(comp_standard_qty as number(38, 10)) as comp_standard_quantity,

            cast(
                comp_perfection_qty as number(38, 10)
            ) as comp_perfection_quantity,

            cast(comp_scrap_percent as number(38, 10)) as comp_scrap_percent,

            cast(
                substring(item_match_bom_flag, 1, 255) as text(255)
            ) as item_match_bom_flag,

            cast(transaction_amt as number(38, 10)) as transaction_amt,

            cast(stock_adj_qty as number(38, 10)) as stock_adj_qty,

            cast(substring(product_class, 1, 255) as text(255)) as product_class,

            cast(
                substring(consolidated_batch_order, 1, 255) as text(255)
            ) as consolidated_batch_order,

            cast(substring(bulk_flag, 1, 255) as text(255)) as bulk_flag,

            cast(trandt_actual_amount as number(38, 10)) as trandt_actual_amount,

            cast(gldt_actual_amount as number(38, 10)) as gldt_actual_amount,

            cast(standard_amount as number(38, 10)) as standard_amount,

            cast(perfection_amount as number(38, 10)) as perfection_amount,

            cast(gldt_stock_adj_amount as number(38, 10)) as gldt_stock_adj_amount,

            cast(load_date as timestamp_ntz(9)) as load_date,

            cast(update_date as timestamp_ntz(9)) as update_date,

            cast(
                substring(comp_item_model_group, 1, 255) as text(255)
            ) as comp_item_model_group,

            cast(
                substring(wo_item_model_group, 1, 255) as text(255)
            ) as wo_item_model_group,

            cast(substring(wo_stock_site, 1, 255) as text(255)) as wo_stock_site,

            cast(substring(flag, 1, 255) as text(255)) as flag,

            cast(gl_date as date) as gl_date
        from final
    )

select
    *,
    {{
        dbt_utils.surrogate_key(
            [
                "source_system",
                "comp_stock_site",
                "financial_site",
                "voucher",
                "work_order_number",
                "comp_src_item_identifier",
                "comp_src_variant_code",
                "transaction_date",
                "comp_item_type",
                "source_bom_identifier",
                "wo_src_item_identifier",
                "source_business_unit_code",
                "company_code",
                "comp_transaction_uom",
                "transaction_currency",
                "product_class",
                "wo_src_variant_code",
                "wo_stock_site",
            ]
        )
    }} as unique_key
from final_with_casting
