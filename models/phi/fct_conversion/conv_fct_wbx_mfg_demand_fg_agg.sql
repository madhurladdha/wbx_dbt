with
    old_fct as (
        select *
        from {{ source("FACTS_FOR_COMPARE", "mfg_wtx_demand_fg_agg") }}
        where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
    ),
converted_fct as (
    select
   cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(snapshot_date as date) as snapshot_date,

    cast(
        substring(source_bom_identifier, 1, 255) as text(255)
    ) as source_bom_identifier,

    cast(substring(week_description, 1, 255) as text(255)) as week_description,

    cast(week_start_dt as date) as week_start_dt,

    cast(week_end_dt as date) as week_end_dt,

    cast(
        substring(planned_unplanned_code, 1, 20) as text(20)
    ) as planned_unplanned_code,

    cast(substring(plan_version, 1, 20) as text(20)) as plan_version,

    cast(
        substring(wo_src_item_identifier, 1, 255) as text(255)
    ) as wo_src_item_identifier,

    cast(substring(wo_variant_code, 1, 255) as text(255)) as wo_variant_code,

    cast(            {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "wo_src_item_identifier",
                    ]
                )
            }} as text(255)) as wo_src_item_guid,

    cast(
        substring(wo_business_unit_code, 1, 255) as text(255)
    ) as wo_business_unit_code,

    cast({{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "wo_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }}  as text(255)) as wo_business_unit_guid,

    cast(wo_planned_qty as number(38, 10)) as wo_planned_qty,

   cast(wo_on_hand_qty as number(38, 10)) as wo_on_hand_qty,


    cast(update_date as date) as update_date

-- cast(unique_key as text(255) ) as unique_key 

from old_fct 
)

Select *        
from converted_fct 
   