{{ config(tags=["wbx", "manufacturing", "work_order", "gl", "agg"]) }}

with
    source as (select * from {{ ref("fct_wbx_mfg_wo_gl_agg") }}),

    renamed as (
        select
            source_system,
            document_company,
            voucher,
            journal_number,
            work_order_number,
            wo_src_item_identifier,
            wo_src_variant_code,
            gl_date,
            wo_stock_site,
            a550010_gl_amount,
            a550015_gl_amount,
            a510045_gl_amount,
            a718020_gl_amount,
            a718040_gl_amount,
            a718060_gl_amount,
            service_transaction_amt,
            transaction_currency,
            receipe_value,
            actual_transaction_amt,
            perfection_amt,
            standard_amt,
            produced_qty,
            bulk_order_flag,
            source_business_unit_code,
            price_variance_amount,
            item_model_group,
            load_date,
            update_date
        from source
    )

select *
from renamed
