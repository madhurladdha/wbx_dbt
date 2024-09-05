{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["sales", "actuals","sales_actuals"],
        transient=false,
        unique_key="UNIQUE_KEY",
        full_refresh=false,
        on_schema_change="sync_all_columns",
        pre_hook="""
                {{ truncate_if_exists(this.schema, this.table) }}
                """,
    )
}}

/*
SOURCE_LEGACY have values AX,D365 and IBERICA_HIST.
D365 indicates current data sourced from D365.
AX corresponds to all AX history data
IBERICA_HIST indicated Iberica history data
*/

with int_view as (
    select
        *,
        'D365' as source_legacy
    from {{ ref('int_f_wbx_sls_order_hdr') }}
),

ibe_hist as (
    select
        *,
        'IBERICA_HIST' as source_legacy
    from {{ ref('fct_wbx_sls_order_hdr_ibehist') }}
),

/* Filtering this down so as to NOT PULL AX HISTORY where the Sales Order are not also in D365.
    If the Sales Order is in D365, that means it was converted in D365 itself and so we must suppress it out of
    AX History.
    This logic is in lieu of the normal unique_key check that is later in the code because the unique_key field is changed in D365 
    data even if the Sales Order are the same.
*/
ax_hist as (select * from {{ ref('conv_fct_wbx_sls_order_hdr') }}
    where (sales_order_company, sales_order_number) not in (select distinct upper(trim(dataareaid)), salesid  
    from {{ref("src_salestable")}})
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
        unique_key,
        source_legacy
    from int_view
),

old_ax_hist as(
select 
a.source_system,
a.sales_order_number,
a.source_sales_order_type,
a.sales_order_type,
a.sales_order_company,
a.source_business_unit_code,
a.business_unit_address_guid,
a.ship_source_customer_code,
a.ship_customer_addr_number_guid,
a.bill_source_customer_code,
a.bill_customer_addr_number_guid,
a.source_base_currency,
a.ordered_date,
a.sched_pick_date,
a.cancelled_date,
a.invoice_date,
a.requested_date,
a.actual_ship_date,
a.actl_ship_reason_code,
a.actl_ship_reason_desc,
a.arrival_date,
a.arrival_reason_code,
a.arrival_reason_desc,
a.revised_crad_date,
a.crad_date,
a.hold_status,
a.carr_trsp_mode_code,
a.source_updated_datetime,
a.load_date,
a.update_date,
a.header_status_code,
a.header_status_desc,
a.unique_key,
a.source_legacy as source_legacy
from ax_hist a
--left join fact b on a.unique_key=b.unique_key where b.source_system is null
/* This left join, which normally ensures that we do not pull rows from AX history that were migrated over to D365 is handled in the earlier filtering logic 
    against the conv model itself.
    This is specific to Sales Order and some other models where the UNIQUE_KEY logic doesn't suffice.
*/
),

final as (
    select * from ibe_hist
    union all
    select * from fact
    union all
    select * from old_ax_hist
)

select * from final
qualify row_number() over (partition by unique_key order by unique_key desc) = 1
