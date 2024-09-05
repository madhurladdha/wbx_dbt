{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx", "manufacturing", "work order", "item", "agg"],
        unique_key='unique_key',
        pre_hook="""
                     {{ truncate_if_exists(this.schema, this.table) }}
                     """,
    )
}}

with
    source as (select * from {{ ref("int_f_wbx_mfg_wo_item_agg") }}),
    final as (
        select *, row_number() over (partition by unique_key order by unique_key) rownum
        from source
    )

select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,

    cast(item_guid as text(255)) as item_guid,

    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,

    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,

    cast(calendar_date as date) as calendar_date,

    cast(substring(work_center_code, 1, 255) as text(255)) as work_center_code,

    cast(substring(work_center_desc, 1, 255) as text(255)) as work_center_desc,

    cast(scheduled_qty as number(20, 4)) as scheduled_qty,

    cast(cancelled_qty as number(20, 4)) as cancelled_qty,

    cast(produced_qty as number(20, 4)) as produced_qty,

    cast(scheduled_kg_qty as number(38, 10)) as scheduled_kg_qty,

    cast(produced_lb_qty as number(38, 10)) as produced_lb_qty,

    cast(orig_scheduled_qty as number(20, 4)) as orig_scheduled_qty,

    cast(orig_scheduled_kg_qty as number(38, 10)) as orig_scheduled_kg_qty,

    cast(produced_kg_qty as number(38, 10)) as produced_kg_qty,

    cast(ctp_target as number(38, 10)) as ctp_target_percent,

    cast(ptp_target as number(38, 10)) as ptp_target_percent,

    cast(unique_key as text(255)) as unique_key
from final
where rownum = 1
