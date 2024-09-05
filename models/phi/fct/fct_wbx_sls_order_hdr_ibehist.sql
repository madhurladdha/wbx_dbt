{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["sales", "actuals_hist","sales_actuals_hist"],
        transient=false,
        snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns",
        pre_hook="""
            {{ truncate_if_exists(this.schema, this.table) }}
            """
    )
}}

with int_view as (
    select * from {{ ref('int_f_wbx_sls_order_hdr_ibehist') }}
),

fact as (
    select 
        source_system,
        sales_order_number,
        source_sales_order_type,
        sales_order_type,
        sales_order_company,
        source_business_unit_code,
        business_unit_address_guid,
        ship_source_customer_code,
        ship_customer_addr_number_guid,
        bill_source_customer_code,
        bill_customer_addr_number_guid,
        source_base_currency,
        ordered_date,
        sched_pick_date,
        cancelled_date,
        invoice_date,
        requested_date,
        actual_ship_date,
        actl_ship_reason_code,
        actl_ship_reason_desc,
        arrival_date,
        arrival_reason_code,
        arrival_reason_desc,
        revised_crad_date,
        crad_date,
        hold_status,
        carr_trsp_mode_code,
        source_updated_datetime,
        load_date,
        update_date,
        header_status_code,
        header_status_desc,
        unique_key
    from int_view
)

select * from fact
qualify row_number() over (partition by unique_key order by unique_key desc)=1
