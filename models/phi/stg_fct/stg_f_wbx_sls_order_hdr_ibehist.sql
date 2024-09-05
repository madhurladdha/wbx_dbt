{{
    config(
        tags=["sales", "actuals_hist","sales_actuals_hist"]
    )
}}

with ibe_history_sales as (select * from {{ ref('src_ibe_history_sales') }}),
final as (
    select
        'WEETABIX' as source_system,
        Sales_Order as sales_order_number,
        Order_Type as source_sales_order_type,
        'IBE' as sales_order_company,
        site as source_business_unit_code,
        Customer_Account as ship_source_customer_code,
        cast(null as varchar2(1)) as bill_source_customer_code,
        TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_Ordered, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))  as ordered_date,
        null as sched_pick_date,
        TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_Cancelled, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))  as cancelled_date,
        TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_Invoiced, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))   as invoice_date,
        null as requested_date,
        TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_Despatched, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))  as actual_ship_date,
        cast(null as varchar2(1)) as actl_ship_reason_code,
        cast(null as varchar2(1)) as actl_ship_reason_desc,
        null as arrival_date,
        cast(null as varchar2(1)) as arrival_reason_code,
        cast(null as varchar2(1)) as arrival_reason_desc,
        null as revised_crad_date,
        null as crad_date,
        cast(null as varchar2(1)) as hold_status,
        cast(null as varchar2(1)) as carr_trsp_mode_code,
        localtimestamp as source_updated_datetime,
        cast(null as varchar2(1)) as header_status_code
    from ibe_history_sales
)
select * from final
