{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx", "manufacturing", "work_order", "gl", "agg"],
        unique_key='unique_key',
        pre_hook="""
                 {{ truncate_if_exists(this.schema, this.table) }}
                 """,
    )
}}

with
    source as (select * from {{ ref("int_f_wbx_mfg_wo_gl_agg") }}),
    final as (
        select *, row_number() over (partition by unique_key order by 1) rownum
        from source
    )

select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(substring(document_company, 1, 255) as text(255)) as document_company,

    cast(substring(voucher, 1, 255) as text(255)) as voucher,

    cast(substring(journal_number, 1, 255) as text(255)) as journal_number,

    cast(substring(work_order_number, 1, 255) as text(255)) as work_order_number,

    cast(
        substring(wo_src_item_identifier, 1, 255) as text(255)
    ) as wo_src_item_identifier,

    cast(substring(wo_src_variant_code, 1, 255) as text(255)) as wo_src_variant_code,

    cast(gl_date as date) as gl_date,

    cast(substring(wo_stock_site, 1, 255) as text(255)) as wo_stock_site,

    cast(a550010_gl_amount as number(22, 7)) as a550010_gl_amount,

    cast(a550015_gl_amount as number(22, 7)) as a550015_gl_amount,

    cast(a510045_gl_amount as number(22, 7)) as a510045_gl_amount,

    cast(a718020_gl_amount as number(22, 7)) as a718020_gl_amount,

    cast(a718040_gl_amount as number(22, 7)) as a718040_gl_amount,

    cast(a718060_gl_amount as number(22, 7)) as a718060_gl_amount,

    cast(service_transaction_amt as number(22, 7)) as service_transaction_amt,

    cast(substring(transaction_currency, 1, 255) as text(255)) as transaction_currency,

    cast(receipe_value as number(22, 7)) as receipe_value,

    cast(actual_transaction_amt as number(22, 7)) as actual_transaction_amt,

    cast(perfection_amt as number(22, 7)) as perfection_amt,

    cast(standard_amt as number(22, 7)) as standard_amt,

    cast(produced_qty as number(22, 7)) as produced_qty,

    cast(substring(bulk_order_flag, 1, 10) as text(10)) as bulk_order_flag,

    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,

    cast(a550030_gl_amount as number(22, 7)) as price_variance_amount,

    cast(substring(item_model_group, 1, 255) as text(255)) as item_model_group,

    cast(load_date as timestamp_ntz(9)) as load_date,

    cast(update_date as timestamp_ntz(9)) as update_date,

    cast(unique_key as text(255)) as unique_key
from final
where rownum = 1
