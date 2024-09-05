{{
    config(
    enabled=true,
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_sales_order"]
    )
}}

----review enable option in config/*not working as expected*/

with old_fct as (
    select *
    from {{ source('WBX_PROD_FACT','fct_wbx_sls_order_hdr') }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
),

old_plant as (
    select
        source_business_unit_code_new,
        source_business_unit_code,
        plantdc_address_guid_new,
        plantdc_address_guid
    from {{ ref('conv_dim_wbx_plant_dc') }}
),

converted_fct as (
    select
        source_system,
        sales_order_number,
        source_sales_order_type,
        sales_order_type,
        sales_order_company,
        a.source_business_unit_code as source_business_unit_code_old,
        plnt.source_business_unit_code_new as source_business_unit_code,
        a.business_unit_address_guid as business_unit_address_guid_old,
        plnt.plantdc_address_guid_new as business_unit_address_guid,
        ship_source_customer_code,
        {{ dbt_utils.surrogate_key(['a.source_system','a.ship_source_customer_code',"'CUSTOMER_MAIN'","a.sales_order_company"]) }} AS ship_customer_addr_number_guid,
        bill_source_customer_code,
        {{dbt_utils.surrogate_key(["a.source_system","ltrim(rtrim(a.bill_source_customer_code))","'CUSTOMER_MAIN'","a.sales_order_company"])}} as bill_customer_addr_number_guid,
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
        unique_key,
        'AX' as source_legacy
    from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid
)

select 
      cast(substring(source_system,1,255) as text(255) ) as source_system  ,
        cast(substring(sales_order_number,1,255) as text(255) ) as sales_order_number  ,
        cast(substring(source_sales_order_type,1,255) as text(255) ) as source_sales_order_type  ,
        cast(substring(sales_order_type,1,255) as text(255) ) as sales_order_type  ,
        cast(substring(sales_order_company,1,20) as text(20) ) as sales_order_company  ,
		cast(substring(source_business_unit_code_old,1,255) as text(255) ) as source_business_unit_code_old,															 
        cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
		cast(business_unit_address_guid_old as text(255) ) as business_unit_address_guid_old ,
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
        cast(unique_key as text(255) ) as unique_key,
		cast(source_legacy as text(15) ) as source_legacy
        from converted_fct