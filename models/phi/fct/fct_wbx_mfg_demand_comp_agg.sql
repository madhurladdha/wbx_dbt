{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx","manufacturing","demand","agg",],
        unique_key="SNAPSHOT_DATE",
        on_schema_change="sync_all_columns",
        incremental_strategy="delete+insert",
        full_refresh=false,
        post_hook="""
        {% if check_table_exists( this.schema, this.table ) == 'True' %}
        update {{this}} s 
        set s.count_fg_occurence = l.count_fg
        from
        (select  comp_src_item_identifier, comp_src_variant_code,snapshot_date,(count (distinct wo_src_item_identifier)) count_fg 
        from {{this}}
        where snapshot_date = (select max(snapshot_date) from {{this}})
        group by comp_src_item_identifier, comp_src_variant_code,snapshot_date) l
        where l.comp_src_item_identifier =s.comp_src_item_identifier
        and l.comp_src_variant_code =s.comp_src_variant_code
        and l.snapshot_date = s.snapshot_date
        and s.snapshot_date = (select max(snapshot_date) from {{this}})
         {% endif %}  
                
        """
    )
}}

with
    old_table as (
        select *
        from {{ ref("conv_fct_wbx_mfg_demand_comp_agg") }}
        {% if check_table_exists(this.schema, this.table) == "False" %}
            limit {{ env_var("DBT_NO_LIMIT") }}  -- --------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
        {% else %} limit {{ env_var("DBT_LIMIT") }}  -- ---Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

        {% endif %}

    ),

    base_fct as (
        select *
        from {{ ref("int_f_wbx_mfg_demand_comp_agg") }}
        {% if check_table_exists(this.schema, this.table) == "True" %}
            limit {{ env_var("DBT_NO_LIMIT") }}
        {% else %} limit {{ env_var("DBT_LIMIT") }}
        {% endif %}
    ),
    old_model as (
        select
            cast(snapshot_date as date) as snapshot_date,

            cast(substring(source_system, 1, 255) as text(255)) as source_system,

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

            cast(
                substring(comp_src_item_identifier, 1, 255) as text(255)
            ) as comp_src_item_identifier,

            cast(comp_src_item_guid as text(255)) as comp_src_item_guid,

            cast(
                substring(comp_business_unit_code, 1, 255) as text(255)
            ) as comp_business_unit_code,

            cast(comp_business_unit_guid as text(255)) as comp_business_unit_guid,

            cast(
                substring(comp_src_variant_code, 1, 255) as text(255)
            ) as comp_src_variant_code,

            cast(comp_required_qty as number(38, 10)) as comp_required_qty,

            cast(count_fg_occurence as number(38, 10)) as count_fg_occurence,

            cast(load_date as date) as load_date,

            cast(update_date as date) as update_date
        from old_table
    ),
    snpt_fact as (
        select
            cast(snapshot_date as date) as snapshot_date,

            cast(substring(source_system, 1, 255) as text(255)) as source_system,

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

            cast(
                substring(comp_src_item_identifier, 1, 255) as text(255)
            ) as comp_src_item_identifier,

            cast(comp_src_item_guid as text(255)) as comp_src_item_guid,

            cast(
                substring(comp_business_unit_code, 1, 255) as text(255)
            ) as comp_business_unit_code,

            cast(comp_business_unit_guid as text(255)) as comp_business_unit_guid,

            cast(
                substring(comp_src_variant_code, 1, 255) as text(255)
            ) as comp_src_variant_code,

            cast(comp_required_qty as number(38, 10)) as comp_required_qty,

            cast(count_fg_occurence as number(38, 10)) as count_fg_occurence,

            cast(load_date as date) as load_date,

            cast(update_date as date) as update_date
        from base_fct bf

    )

select *
from snpt_fact
union
select *
from old_model
