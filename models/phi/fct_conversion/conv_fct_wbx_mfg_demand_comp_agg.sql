with
    old_fct as (
        select * from {{ source("FACTS_FOR_COMPARE", "mfg_wtx_demand_comp_agg") }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
    ),
    converted_fct as (
    select
    cast(snapshot_date as date) as snapshot_date  ,

    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_bom_identifier,1,255) as text(255) ) as source_bom_identifier  ,

    cast(substring(week_description,1,255) as text(255) ) as week_description  ,

    cast(week_start_dt as date) as week_start_dt  ,

    cast(week_end_dt as date) as week_end_dt  ,

    cast(substring(planned_unplanned_code,1,20) as text(20) ) as planned_unplanned_code  ,

    cast(substring(plan_version,1,20) as text(20) ) as plan_version  ,

    cast(substring(wo_src_item_identifier,1,255) as text(255) ) as wo_src_item_identifier  ,

    cast(substring(wo_variant_code,1,255) as text(255) ) as wo_variant_code  ,

    cast({{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "wo_src_item_identifier",
                    ]
                )
        }} as text(255) ) as wo_src_item_guid ,

    cast(substring(wo_business_unit_code,1,255) as text(255) ) as wo_business_unit_code  ,

    cast(   {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "wo_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }} as text(255) ) as wo_business_unit_guid  ,

    cast(substring(comp_src_item_identifier,1,255) as text(255) ) as comp_src_item_identifier  ,

    cast(            {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "comp_src_item_identifier",
                    ]
                )
            }} as text(255) ) as comp_src_item_guid  ,

    cast(substring(comp_business_unit_code,1,255) as text(255) ) as comp_business_unit_code  ,

    cast(  {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "comp_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }} as text(255) ) as comp_business_unit_guid  ,

    cast(substring(comp_src_variant_code,1,255) as text(255) ) as comp_src_variant_code  ,

    cast(comp_required_qty as number(38,10) ) as comp_required_qty  ,

    cast(count_fg_occurence as number(38,10) ) as count_fg_occurence  ,

    cast(load_date as date) as load_date  ,

    cast(update_date as date) as update_date  
        from old_fct
    )

select *
from converted_fct
