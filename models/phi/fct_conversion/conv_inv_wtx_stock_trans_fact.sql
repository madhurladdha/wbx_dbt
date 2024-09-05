{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    query_tag='test_conversion',
    )
}}

WITH old_fct AS 
(
    SELECT * FROM {{source('FACTS_FOR_COMPARE','inv_wtx_stock_trans_fact')}} WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}'
),

converted_fct as (
    select
        source_system,
        source_transaction_key,
        source_record_id,
        related_document_number,
        source_item_identifier,
        item_guid,
        source_business_unit_code,
        business_unit_address_guid,
        variant_code,
        transaction_date,
        gl_date,
        transaction_qty,
        transaction_amt,
        transaction_uom,
        transaction_currency,
        status_code,
        status_desc,
        voucher,
        adjustment_amt,
        update_date,
        company_code,
        site,
        product_class,
        load_date,
        stock_site,
        invoice_returned_flag,
        item_model_group
    from old_fct 
    )

select
    *,
    {{
        dbt_utils.surrogate_key(
            [
                "cast(substring(source_system,1,255) as text(255) )",
                "cast(substring(source_transaction_key,1,255) as text(255) )",
                "cast(source_record_id as number(38,0) )",
                "cast(substring(related_document_number,1,255) as text(255) )",
                "cast(substring(source_item_identifier,1,255) as text(255) )",
                "cast(substring(source_business_unit_code,1,255) as text(255) )",
                "cast(substring(variant_code,1,255) as text(255) )",
                "cast(gl_date as timestamp_ntz(9) )"
            ]
        )
    }} as unique_key
from converted_fct