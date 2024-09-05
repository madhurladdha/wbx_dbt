{{ config(tags=["wbx", "manufacturing", "yield", "agg"]) }}


with
    source as (

        select * from {{ source("FACTS_FOR_COMPARE", "mfg_wtx_yield_agg_fact") }}

    ),

    renamed as (

        select
            source_system,
            comp_stock_site,
            financial_site,
            voucher,
            work_order_number,
            comp_src_item_identifier,
            comp_src_variant_code,
            transaction_date,
            comp_item_type,
            source_bom_identifier,
            wo_src_item_identifier,
            wo_src_variant_code,
            source_business_unit_code,
            company_code,
            comp_transaction_uom,
            transaction_currency,
            actual_transaction_qty,
            comp_standard_quantity,
            comp_perfection_quantity,
            comp_scrap_percent,
            item_match_bom_flag,
            transaction_amt,
            stock_adj_qty,
            product_class,
            consolidated_batch_order,
            bulk_flag,
            trandt_actual_amount,
            gldt_actual_amount,
            standard_amount,
            perfection_amount,
            gldt_stock_adj_amount,
            load_date,
            update_date,
            comp_item_model_group,
            wo_item_model_group,
            wo_stock_site,
            flag,
            gl_date

        from source

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
from renamed
