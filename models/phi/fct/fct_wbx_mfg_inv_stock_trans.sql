{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx", "manufacturing","stock","yield"],
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        transient=false,
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns",
        pre_hook="""
            {% set now = modules.datetime.datetime.now() %}
            {%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
            {%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}
            {% if day_today == full_load_day %}
            {{ truncate_if_exists(this.schema, this.table) }}
            {% endif %}
            """,
    )
}}

with int_view as (
    select * from {{ ref('int_f_wbx_mfg_inv_stock_trans') }}
),

fact as (
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
        item_model_group,
        unique_key
    from int_view
)

select * from fact
qualify row_number() over (partition by unique_key order by unique_key desc)=1