{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx", "manufacturing", "yield", "stock_adj", "inventory"],
        unique_key="unique_key",
        on_schema_change="sync_all_columns",
        full_refresh=false,
    )
}}

with
    new_table as (select * from {{ ref("int_f_wbx_mfg_inv_stock_adj") }}),
    -- new fact with casting
    new_fct as (
        select
            cast(substring(source_system, 1, 255) as text(255)) as source_system,

            cast(
                substring(source_account_identifier, 1, 255) as text(255)
            ) as source_account_identifier,

            cast(source_record_id as number(38, 0)) as source_record_id,

            cast(substring(company_code, 1, 255) as text(255)) as company_code,

            cast(
                substring(related_document_type, 1, 255) as text(255)
            ) as related_document_type,

            cast(
                substring(related_document_desc, 1, 255) as text(255)
            ) as related_document_desc,

            cast(
                substring(related_document_number, 1, 255) as text(255)
            ) as related_document_number,

            cast(substring(site, 1, 255) as text(255)) as site,

            cast(substring(product_class, 1, 255) as text(255)) as product_class,

            cast(
                substring(source_item_identifier, 1, 255) as text(255)
            ) as source_item_identifier,

            cast(item_guid as text(255)) as item_guid,

            cast(
                substring(source_business_unit_code, 1, 255) as text(255)
            ) as source_business_unit_code,

            cast(plantdc_address_guid as text(255)) as business_unit_address_guid,

            cast(substring(variant_code, 1, 255) as text(255)) as variant_code,

            cast(transaction_date as date) as transaction_date,

            cast(gl_date as timestamp_ntz(9)) as gl_date,

            cast(transaction_qty as number(20, 4)) as transaction_qty,

            cast(transaction_amt as number(20, 4)) as transaction_amt,

            cast(substring(transaction_uom, 1, 255) as text(255)) as transaction_uom,

            cast(
                substring(transaction_currency, 1, 255) as text(255)
            ) as transaction_currency,

            cast(substring(voucher, 1, 255) as text(255)) as voucher,

            cast(load_date as date) as load_date,

            cast(update_date as date) as update_date,

            cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,

            cast(substring(stock_site, 1, 255) as text(255)) as stock_site,

            cast(unique_key as text(255)) as unique_key
        from new_table
        qualify row_number() over (partition by unique_key order by 1) = 1
    )

select *
from new_fct
