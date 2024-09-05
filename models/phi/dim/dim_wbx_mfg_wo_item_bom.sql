{{ 
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["wbx", "manufacturing", "bom", "work order"],
     unique_key='unique_key', 
    on_schema_change='sync_all_columns', 
    pre_hook="""
            {% if check_table_exists( this.schema, this.table ) == 'True' %}
            truncate table {{ this }}
            {% endif %}          
            """,
    ) 
}} 


with
    src as (
        select *, row_number() over (partition by unique_key order by 1) rownum
        from {{ ref("int_d_wbx_mfg_wo_item_bom") }}
    )

    select   source_system,
        source_bom_identifier,
        source_bom_name,
        bom_identifier_path,
        comp_bom_identifier,
        comp_bom_identifier_linenum,
        root_company_code,
        root_src_item_identifier,
        root_src_variant_code,
        root_src_item_guid,
        root_business_unit_code,
        parent_src_item_identifier,
        parent_src_item_guid,
        parent_src_variant_code,
        comp_src_item_identifier,
        comp_src_item_guid,
        comp_src_variant_code,
        comp_required_qty,
        root_qty,
        comp_scrap_percent,
        root_scrap_percent,
        comp_item_uom,
        root_item_uom,
        parent_item_uom,
        effective_date,
        expiration_date,
        active_flag,
        bom_level,
        bom_path,
        base_item_std_cost,
        pcomp_item_std_cost,
        base_item_last_cost,
        pcomp_item_last_cost,
        source_updated_date,
        load_date,
        update_date,
        comp_perfection_qty,
        item_model_group,
        unique_key
        from src 
        where rownum = 1