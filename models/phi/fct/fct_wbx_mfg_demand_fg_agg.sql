{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx","manufacturing","demand","fg","agg",],
        unique_key="SNAPSHOT_DATE",
        on_schema_change="sync_all_columns",
        incremental_strategy="delete+insert",
        full_refresh=false,
        )
}}

with
    old_table as (
        select *
        from {{ ref("conv_fct_wbx_mfg_demand_fg_agg") }}
        {% if check_table_exists(this.schema, this.table) == "False" %}
            limit {{ env_var("DBT_NO_LIMIT") }}  -- --------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
        {% else %} limit {{ env_var("DBT_LIMIT") }}  -- ---Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

        {% endif %}

    ),

    base_fct as (
        select *
        from {{ ref("int_f_wbx_mfg_demand_fg_agg") }}
        {% if check_table_exists(this.schema, this.table) == "True" %}
            limit {{ env_var("DBT_NO_LIMIT") }}
        {% else %} limit {{ env_var("DBT_LIMIT") }}
        {% endif %}
    ),
    old_model as (
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

    cast(wo_src_item_guid as text(255)) as wo_src_item_guid,

    cast(
        substring(wo_business_unit_code, 1, 255) as text(255)
    ) as wo_business_unit_code,

    cast(wo_business_unit_guid as text(255)) as wo_business_unit_guid,

    cast(wo_planned_qty as number(38, 10)) as wo_planned_qty,

   cast(wo_on_hand_qty as number(38, 10)) as wo_on_hand_qty,


    cast(update_date as date) as update_date

-- cast(unique_key as text(255) ) as unique_key 
        from old_table
    ),
    snpt_fact as (
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

    cast(wo_src_item_guid as text(255)) as wo_src_item_guid,

    cast(
        substring(wo_business_unit_code, 1, 255) as text(255)
    ) as wo_business_unit_code,

    cast(wo_business_unit_guid as text(255)) as wo_business_unit_guid,

    cast(wo_planned_qty as number(38, 10)) as wo_planned_qty,

   cast(wo_on_hand_qty as number(38, 10)) as wo_on_hand_qty,


    cast(update_date as date) as update_date

-- cast(unique_key as text(255) ) as unique_key 
        from base_fct bf

    )

select *
from snpt_fact
union
select *
from old_model