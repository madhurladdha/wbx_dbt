{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx", "manufacturing", "yield", "agg"],
        unique_key="unique_key",
        pre_hook="""
                 {{ truncate_if_exists(this.schema, this.table) }}
                 """,
    )
}}

with
    source as (select * from {{ ref("int_f_wbx_mfg_yield_agg") }}),
    final as (
        select *, row_number() over (partition by unique_key order by 1) rownum
        from source
    )

select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(substring(comp_stock_site, 1, 255) as text(255)) as comp_stock_site,

    cast(substring(financial_site, 1, 255) as text(255)) as financial_site,

    cast(substring(voucher, 1, 255) as text(255)) as voucher,

    cast(substring(work_order_number, 1, 255) as text(255)) as work_order_number,

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

    cast(substring(wo_src_variant_code, 1, 255) as text(255)) as wo_src_variant_code,

    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,

    cast(substring(company_code, 1, 255) as text(255)) as company_code,

    cast(substring(comp_transaction_uom, 1, 255) as text(255)) as comp_transaction_uom,

    cast(substring(transaction_currency, 1, 255) as text(255)) as transaction_currency,

    cast(actual_transaction_qty as number(38, 10)) as actual_transaction_qty,

    cast(comp_standard_quantity as number(38, 10)) as comp_standard_quantity,

    cast(comp_perfection_quantity as number(38, 10)) as comp_perfection_quantity,

    cast(comp_scrap_percent as number(38, 10)) as comp_scrap_percent,

    cast(substring(item_match_bom_flag, 1, 255) as text(255)) as item_match_bom_flag,

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

    cast(substring(wo_item_model_group, 1, 255) as text(255)) as wo_item_model_group,

    cast(substring(wo_stock_site, 1, 255) as text(255)) as wo_stock_site,

    cast(substring(flag, 1, 255) as text(255)) as flag,

    cast(gl_date as date) as gl_date,

    cast(unique_key as text(255)) as unique_key
from final
where rownum = 1
