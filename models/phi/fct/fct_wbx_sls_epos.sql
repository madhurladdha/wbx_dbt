{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["sales", "epos","sls_epos"],
        pre_hook="""
                {{ truncate_if_exists(this.schema, this.table) }}
                """,
    )
}}

with
    source as (select * from {{ ref("int_f_wbx_sls_epos") }}),
    final as (
        select *, row_number() over (partition by unique_key order by unique_key) rownum
        from source
    )

select
    cast(unique_key as text(255)) as unique_key,
    cast(substring(source_system, 1, 10) as text(10)) as source_system,
    cast(item_guid as text(255)) as item_guid,
    cast( substring(source_item_identifier, 1, 255) as text(255) ) as source_item_identifier,
    cast(substring(trade_type, 1, 255) as text(255)) as trade_type_code,
    cast(substring(bill_source_customer_code, 1, 255) as text(255) ) as bill_source_customer_code,
    cast(calendar_week as number(38, 0)) as calendar_week,
    cast(calendar_date as timestamp_ntz(9)) as calendar_date,
    cast(fiscal_period_no as number(38, 0)) as fiscal_period_number,
    cast(substring(primary_uom, 1, 255) as text(255)) as primary_uom,
    cast(bill_customer_address_guid as text(255)) as bill_customer_address_guid,
    cast(o_qty_ca as number(38, 10)) as qty_ca,
    cast(qty_kg as number(38, 10)) as qty_kg,
    cast(qty_ul as number(38, 10)) as qty_ul,
    cast(qty_prim as number(38, 10)) as qty_prim,
    cast(number_of_days as number(38, 0)) as number_of_days

from final
where rownum = 1
