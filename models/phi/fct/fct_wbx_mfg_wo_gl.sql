{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags = ["wbx","manufacturing","work order","gl","yield"],
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        transient=false,
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns",
        pre_hook="""
            {{ truncate_if_exists(this.schema, this.table) }}
            """,
    )
}}

with int_view as (
    select * from {{ ref('int_f_wbx_mfg_wo_gl') }}
),

fact as (
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
        recipecalc_date,
        unique_key
    from int_view
)

select * from fact
qualify row_number() over (partition by unique_key order by unique_key desc)=1