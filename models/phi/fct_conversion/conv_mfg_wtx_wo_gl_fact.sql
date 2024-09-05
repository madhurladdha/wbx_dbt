{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    query_tag='test_conversion',
    )
}}

WITH old_fct AS 
(
    SELECT * FROM {{source('FACTS_FOR_COMPARE','mfg_wtx_wo_gl_fact')}} WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),
converted_fct as (
    select 
        source_system,
        document_company,
        document_type,
        document_number,
        voucher,
        journal_number,
        reference_id,
        gl_date,
        source_site_code,
        source_business_unit_code,
        cost_center_code,
        source_date_updated,
        load_date,
        update_date,
        source_updated_d_id,
        product_class,
        source_account_identifier,
        account_guid,
        transaction_amount,
        transaction_currency,
        remark_txt,
        recipecalc_date
    from old_fct
)

select
    *,
    {{
        dbt_utils.surrogate_key(
            [
                "cast(substring(source_system,1,255) as text(255) )",
                "cast(substring(document_company,1,255) as text(255) )",
                "cast(substring(document_type,1,255) as text(255) )",
                "cast(substring(document_number,1,255) as text(255) )",
            ]
        )
    }} as unique_key 
from converted_fct