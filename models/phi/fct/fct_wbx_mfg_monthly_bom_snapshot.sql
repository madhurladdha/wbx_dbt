{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["wbx", "manufacturing", "bom", "work order"] ,
    unique_key='snapshot_date', 
    on_schema_change='sync_all_columns', 
    incremental_strategy='delete+insert',
    full_refresh=false,

     pre_hook="""
            {% if check_table_exists( this.schema, this.table ) == 'True' %}
            DELETE FROM {{ this }} WHERE BOM_FLAG='D'
            {% endif %}         
            """,
    )
}}



/* Approach Used: Static Snapshot w/ Historical Conversion
    The approach used for this table is a Snapshot approach but also requires historical conversion from the old IICS data sets.
    Full details can be found in applicable documentation, but the highlights are provided here.
    1) References the old "conversion" or IICS data set for all snapshots up to the migration date.
    2) Environment variables used to drive the filtering so that the IICS data set is only pulled in on the initial run of the model in a new db/env.
    3) Same variables are used to drive filtering on the new (go-forward) data set
    4) End result should be that all old snapshots are captured and then this dbt model appends each new snapshot/version date to the data set in the dbt model.
    Other Design features:
    1) Model should NEVER be allowed to full-refresh.  This could wipe out all history.
    2) Model is incremental with unique_key = version date.  This ensures that past version dates are never deleted and re-runs on the same day will simply delete for
        the given version date and reload.
*/

with old_table as
(
    select * from {{ref('conv_fct_wbx_mfg_monthly_bom_snapshot')}}  /*commenting out the below condition as we want to load the history in any case for first time after cutover*/
    --{% if check_table_exists( this.schema, this.table ) == 'False' %}
    -- limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if model is not present.
   -- {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if eff table exist

--{% endif %}

),

base_fct  as (
    select *,'D365' as source_legacy from {{ ref ('int_f_wbx_mfg_monthly_bom_snapshot') }}
    ----{% if check_table_exists( this.schema, this.table ) == 'True' %}
    --- limit {{env_var('DBT_NO_LIMIT')}}
    ---{% else %} limit {{env_var('DBT_LIMIT')}}
    ---{% endif %}
),


old_model as (
    select
        cast(snapshot_date as date) as snapshot_date  ,

    cast(mnth_effective_date as date) as mnth_effective_date  ,

    cast(mnth_expiration_date as date) as mnth_expiration_date  ,

    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_bom_identifier,1,255) as text(255) ) as source_bom_identifier  ,

    cast(substring(source_bom_name,1,255) as text(255) ) as source_bom_name  ,

    cast(substring(bom_identifier_path,1,4000) as text(4000) ) as bom_identifier_path  ,

    cast(substring(comp_bom_identifier,1,255) as text(255) ) as comp_bom_identifier  ,

    cast(comp_bom_identifier_linenum as number(38,10) ) as comp_bom_identifier_linenum  ,

    cast(substring(root_company_code,1,255) as text(255) ) as root_company_code  ,

    cast(substring(root_src_item_identifier,1,255) as text(255) ) as root_src_item_identifier  ,

    cast(substring(root_src_variant_code,1,255) as text(255) ) as root_src_variant_code  ,

    cast(root_src_item_guid as text(255) ) as root_src_item_guid  ,

    cast(substring(root_business_unit_code,1,255) as text(255) ) as root_business_unit_code  ,

    cast(substring(parent_src_item_identifier,1,255) as text(255) ) as parent_src_item_identifier  ,

    cast(parent_src_item_guid as text(255) ) as parent_src_item_guid  ,

    cast(substring(parent_src_variant_code,1,255) as text(255) ) as parent_src_variant_code  ,

    cast(substring(comp_src_item_identifier,1,255) as text(255) ) as comp_src_item_identifier  ,

    cast(comp_src_item_guid as text(255) ) as comp_src_item_guid  ,

    cast(substring(comp_src_variant_code,1,255) as text(255) ) as comp_src_variant_code  ,

    cast(comp_required_qty as number(38,10) ) as comp_required_qty  ,

    cast(comp_perfection_qty as number(38,10) ) as comp_perfection_qty  ,

    cast(root_qty as number(38,10) ) as root_qty  ,

    cast(comp_scrap_percent as number(38,10) ) as comp_scrap_percent  ,

    cast(root_scrap_percent as number(38,10) ) as root_scrap_percent  ,

    cast(substring(comp_item_uom,1,10) as text(10) ) as comp_item_uom  ,

    cast(substring(root_item_uom,1,10) as text(10) ) as root_item_uom  ,

    cast(substring(parent_item_uom,1,10) as text(10) ) as parent_item_uom  ,

    cast(substring(active_flag,1,4) as text(4) ) as active_flag  ,

    cast(bom_level as number(15,0) ) as bom_level  ,

    cast(substring(bom_path,1,1024) as text(1024) ) as bom_path  ,

    cast(base_item_std_cost as number(38,10) ) as base_item_std_cost  ,

    cast(pcomp_item_std_cost as number(38,10) ) as pcomp_item_std_cost  ,

    cast(base_item_last_cost as number(38,10) ) as base_item_last_cost  ,

    cast(pcomp_item_last_cost as number(38,10) ) as pcomp_item_last_cost  ,

    cast(source_updated_date as date) as source_updated_date  ,

    cast(load_date as date) as load_date  ,

    cast(update_date as date) as update_date  ,

    cast(substring(bom_flag,1,10) as text(10) ) as bom_flag  ,

    cast(substring(item_model_group,1,255) as text(255) ) as item_model_group,

    cast(substring(source_legacy,1,15) as text(15) ) as source_legacy 
        from old_table
),

snpt_fact as (
        select
        cast(snapshot_date as date) as snapshot_date  ,

    cast(mnth_effective_date as date) as mnth_effective_date  ,

    cast(mnth_expiration_date as date) as mnth_expiration_date  ,

    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_bom_identifier,1,255) as text(255) ) as source_bom_identifier  ,

    cast(substring(source_bom_name,1,255) as text(255) ) as source_bom_name  ,

    cast(substring(bom_identifier_path,1,4000) as text(4000) ) as bom_identifier_path  ,

    cast(substring(comp_bom_identifier,1,255) as text(255) ) as comp_bom_identifier  ,

    cast(comp_bom_identifier_linenum as number(38,10) ) as comp_bom_identifier_linenum  ,

    cast(substring(root_company_code,1,255) as text(255) ) as root_company_code  ,

    cast(substring(root_src_item_identifier,1,255) as text(255) ) as root_src_item_identifier  ,

    cast(substring(root_src_variant_code,1,255) as text(255) ) as root_src_variant_code  ,

    cast(root_src_item_guid as text(255) ) as root_src_item_guid  ,

    cast(substring(root_business_unit_code,1,255) as text(255) ) as root_business_unit_code  ,

    cast(substring(parent_src_item_identifier,1,255) as text(255) ) as parent_src_item_identifier  ,

    cast(parent_src_item_guid as text(255) ) as parent_src_item_guid  ,

    cast(substring(parent_src_variant_code,1,255) as text(255) ) as parent_src_variant_code  ,

    cast(substring(comp_src_item_identifier,1,255) as text(255) ) as comp_src_item_identifier  ,

    cast(comp_src_item_guid as text(255) ) as comp_src_item_guid  ,

    cast(substring(comp_src_variant_code,1,255) as text(255) ) as comp_src_variant_code  ,

    cast(comp_required_qty as number(38,10) ) as comp_required_qty  ,

    cast(comp_perfection_qty as number(38,10) ) as comp_perfection_qty  ,

    cast(root_qty as number(38,10) ) as root_qty  ,

    cast(comp_scrap_percent as number(38,10) ) as comp_scrap_percent  ,

    cast(root_scrap_percent as number(38,10) ) as root_scrap_percent  ,

    cast(substring(comp_item_uom,1,10) as text(10) ) as comp_item_uom  ,

    cast(substring(root_item_uom,1,10) as text(10) ) as root_item_uom  ,

    cast(substring(parent_item_uom,1,10) as text(10) ) as parent_item_uom  ,

    cast(substring(active_flag,1,4) as text(4) ) as active_flag  ,

    cast(bom_level as number(15,0) ) as bom_level  ,

    cast(substring(bom_path,1,1024) as text(1024) ) as bom_path  ,

    cast(base_item_std_cost as number(38,10) ) as base_item_std_cost  ,

    cast(pcomp_item_std_cost as number(38,10) ) as pcomp_item_std_cost  ,

    cast(base_item_last_cost as number(38,10) ) as base_item_last_cost  ,

    cast(pcomp_item_last_cost as number(38,10) ) as pcomp_item_last_cost  ,

    cast(source_updated_date as date) as source_updated_date  ,

    cast(load_date as date) as load_date  ,

    cast(update_date as date) as update_date  ,

    cast(substring(bom_flag,1,10) as text(10) ) as bom_flag  ,

    cast(substring(item_model_group,1,255) as text(255) ) as item_model_group,
    cast(substring(source_legacy,1,15) as text(15) ) as source_legacy 
 from base_fct
    
)

select * from snpt_fact
union all
select * from old_model