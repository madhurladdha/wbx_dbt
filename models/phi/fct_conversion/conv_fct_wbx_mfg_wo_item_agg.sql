{{ config(tags=["wbx", "manufacturing", "work order", "item", "agg"]) }}

with
    source as (select * from {{ source("FACTS_FOR_COMPARE", "mfg_wtx_wo_item_agg") }}),

    renamed as (

        select
            source_system,
            source_item_identifier,
            item_guid,
            source_business_unit_code,
            business_unit_address_guid,
            calendar_date,
            work_center_code,
            work_center_desc,
            scheduled_qty,
            cancelled_qty,
            produced_qty,
            scheduled_kg_qty,
            produced_lb_qty,
            orig_scheduled_qty,
            orig_scheduled_kg_qty,
            produced_kg_qty,
            ctp_target_percent,
            ptp_target_percent

        from source

    )

select
    *,
    {{
        dbt_utils.surrogate_key(
            [
                "source_system",
                "source_business_unit_code",
                "source_item_identifier",
                "calendar_date",
                "work_center_code",
            ]
        )
    }} as unique_key
from renamed
