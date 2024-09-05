{{
    config(
        tags=["sales", "actuals_hist","sales_actuals_hist"]
    )
}}

with stage_table as (
    select * from {{ ref('stg_f_wbx_sls_order_hdr_ibehist') }}
),

ref_effective_currency_dim as (
    select distinct
        source_system,
        company_code,
        effective_date,
        expiration_date,
        company_default_currency_code
    from {{ ref("src_ref_effective_currency_dim") }} where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
),

source as (
    select 
        source_system,
        sales_order_number,
        source_sales_order_type,
        sales_order_company,
        source_business_unit_code,
        ship_source_customer_code,
        bill_source_customer_code,
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
        header_status_code
    from stage_table
),

trans_source as (
    select 
        source.*,
        {{dbt_utils.surrogate_key(["source.source_system","ltrim(rtrim(source.source_business_unit_code))","'PLANT_DC'"])}} as business_unit_address_guid,
        {{dbt_utils.surrogate_key(["source.source_system","ltrim(rtrim(source.ship_source_customer_code))","'CUSTOMER_MAIN'","source.sales_order_company"])}} as ship_customer_addr_number_guid, 
        {{dbt_utils.surrogate_key(["source.source_system","ltrim(rtrim(source.bill_source_customer_code))","'CUSTOMER_MAIN'","source.sales_order_company"])}} as bill_customer_addr_number_guid,
        ref_normalization_xref_sls_ordr_typ.normalized_value as sales_order_type,
        ref_normalization_xref_hdr_stat_desc.normalized_value as header_status_desc,
        ref_effective_currency_dim.company_default_currency_code as source_base_currency,
        current_timestamp() as load_date,
        current_timestamp() as update_date
    from source
    left join ref_effective_currency_dim
        on ref_effective_currency_dim.source_system = source.source_system
        and ref_effective_currency_dim.company_code = source.sales_order_company
        and ref_effective_currency_dim.effective_date <= source.ordered_date
        and ref_effective_currency_dim.expiration_date >= source.ordered_date     
    left join {{ ent_dbt_package.lkp_normalization('source.source_system','LOGISTICS','SALES_ORDER_CODE','ltrim(rtrim(source_sales_order_type))','ref_normalization_xref_sls_ordr_typ') }}
    left join {{ ent_dbt_package.lkp_normalization('source.source_system','LOGISTICS','STATUS_CODE_DESC','ltrim(rtrim(header_status_code))','ref_normalization_xref_hdr_stat_desc') }}

),

gen_unique_key as (
    select
        trans_source.*,
        {{
            dbt_utils.surrogate_key(
                [
                    "cast(substring(source_system,1,255) as text(255) )",
                    "cast(substring(sales_order_number,1,255) as text(255) )",
                    "cast(substring(source_sales_order_type,1,20) as text(20) )",
                    "cast(substring(sales_order_company,1,20) as text(20) )",
                ]
            )
        }} as unique_key
    from trans_source
),

final as (
    select 
        cast(substring(source_system,1,255) as text(255) ) as source_system  ,
        cast(substring(sales_order_number,1,255) as text(255) ) as sales_order_number  ,
        cast(substring(source_sales_order_type,1,255) as text(255) ) as source_sales_order_type  ,
        cast(substring(sales_order_type,1,255) as text(255) ) as sales_order_type  ,
        cast(substring(sales_order_company,1,20) as text(20) ) as sales_order_company  ,
        cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
        cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,
        cast(substring(ship_source_customer_code,1,255) as text(255) ) as ship_source_customer_code  ,
        cast(ship_customer_addr_number_guid as text(255) ) as ship_customer_addr_number_guid  ,
        cast(substring(bill_source_customer_code,1,255) as text(255) ) as bill_source_customer_code  ,
        cast(bill_customer_addr_number_guid as text(255) ) as bill_customer_addr_number_guid  ,
        cast(substring(source_base_currency,1,255) as text(255) ) as source_base_currency  ,
        cast(ordered_date as timestamp_ntz(9) ) as ordered_date  ,
        cast(sched_pick_date as timestamp_ntz(9) ) as sched_pick_date  ,
        cast(cancelled_date as timestamp_ntz(9) ) as cancelled_date  ,
        cast(invoice_date as timestamp_ntz(9) ) as invoice_date  ,
        cast(requested_date as timestamp_ntz(9) ) as requested_date  ,
        cast(actual_ship_date as timestamp_ntz(9) ) as actual_ship_date  ,
        cast(substring(actl_ship_reason_code,1,255) as text(255) ) as actl_ship_reason_code  ,
        cast(substring(actl_ship_reason_desc,1,255) as text(255) ) as actl_ship_reason_desc  ,
        cast(arrival_date as timestamp_ntz(9) ) as arrival_date  ,
        cast(substring(arrival_reason_code,1,255) as text(255) ) as arrival_reason_code  ,
        cast(substring(arrival_reason_desc,1,255) as text(255) ) as arrival_reason_desc  ,
        cast(revised_crad_date as timestamp_ntz(9) ) as revised_crad_date  ,
        cast(crad_date as timestamp_ntz(9) ) as crad_date  ,
        cast(substring(hold_status,1,4) as text(4) ) as hold_status  ,
        cast(substring(carr_trsp_mode_code,1,255) as text(255) ) as carr_trsp_mode_code  ,
        cast(source_updated_datetime as timestamp_ntz(9) ) as source_updated_datetime  ,
        cast(load_date as timestamp_ntz(9) ) as load_date  ,
        cast(update_date as timestamp_ntz(9) ) as update_date  ,
        cast(header_status_code as number(38,0) ) as header_status_code  ,
        cast(substring(header_status_desc,1,255) as text(255) ) as header_status_desc  ,
        cast(unique_key as text(255) ) as unique_key 
    from gen_unique_key
)

select * from final
