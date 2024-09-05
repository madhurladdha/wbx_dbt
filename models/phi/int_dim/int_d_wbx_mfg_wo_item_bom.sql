{{
    config(
        tags=["wbx", "manufacturing", "bom", "work order"],
        
    )
}}

with
    stg_d_wbx_mfg_wo_item_bom as (select * from {{ ref("stg_d_wbx_mfg_wo_item_bom") }}),

    final as (
        select
            source_bom_identifier,
            source_bom_name,

            comp_bom_identifier,
            comp_bom_identifier_linenum,
            upper(root_company_code) as root_company_code,
            root_src_variant_code,            
            root_business_unit_code,
            parent_src_item_identifier,           
            parent_src_variant_code,
            comp_src_item_identifier,            
            comp_src_variant_code,
            comp_required_qty,
            root_qty,
            comp_scrap_percent,
            comp_item_uom,
            root_item_uom,
            parent_item_uom,
            effective_date,
            expiration_date,
            cast(substring(active_flag, 1, 4) as text(4)) as active_flag,
            bom_level,
            bom_path,
            base_item_std_cost,
            pcomp_item_std_cost,
            base_item_last_cost,
            pcomp_item_last_cost,
            source_updated_date,
            comp_perfection_qty,
            item_model_group,
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            root_src_identiftier as root_src_item_identifier,
            bom_indentifier_path as bom_identifier_path,
            parent_scrap_percent as root_scrap_percent,
            {{ dbt_utils.surrogate_key(["source_system", "root_src_item_identifier"]) }}
            as root_src_item_guid,
            {{
                dbt_utils.surrogate_key(
                    ["source_system", "parent_src_item_identifier"]
                )
            }} as parent_src_item_guid,
            {{ dbt_utils.surrogate_key(["source_system", "comp_src_item_identifier"]) }}
            as comp_src_item_guid,
            current_date as load_date,
            current_date as update_date
          
        from stg_d_wbx_mfg_wo_item_bom
    )

select

    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(
        substring(source_bom_identifier, 1, 255) as text(255)
    ) as source_bom_identifier,

    cast(substring(source_bom_name, 1, 255) as text(255)) as source_bom_name,

    cast(substring(bom_identifier_path, 1, 4000) as text(4000)) as bom_identifier_path,

    cast(substring(comp_bom_identifier, 1, 255) as text(255)) as comp_bom_identifier,

    cast(comp_bom_identifier_linenum as number(38, 10)) as comp_bom_identifier_linenum,

    cast(substring(root_company_code, 1, 255) as text(255)) as root_company_code,

    cast(
        substring(root_src_item_identifier, 1, 255) as text(255)
    ) as root_src_item_identifier,

    cast(
        substring(root_src_variant_code, 1, 255) as text(255)
    ) as root_src_variant_code,

    cast(root_src_item_guid as text(255)) as root_src_item_guid,

    cast(
        substring(root_business_unit_code, 1, 255) as text(255)
    ) as root_business_unit_code,

    cast(
        substring(parent_src_item_identifier, 1, 255) as text(255)
    ) as parent_src_item_identifier,

    cast(parent_src_item_guid as text(255)) as parent_src_item_guid,

    cast(
        substring(parent_src_variant_code, 1, 255) as text(255)
    ) as parent_src_variant_code,

    cast(
        substring(comp_src_item_identifier, 1, 255) as text(255)
    ) as comp_src_item_identifier,

    cast(comp_src_item_guid as text(255)) as comp_src_item_guid,

    cast(
        substring(comp_src_variant_code, 1, 255) as text(255)
    ) as comp_src_variant_code,

    cast(comp_required_qty as number(38, 10)) as comp_required_qty,

    cast(root_qty as number(38, 10)) as root_qty,

    cast(comp_scrap_percent as number(38, 10)) as comp_scrap_percent,

    cast(root_scrap_percent as number(38, 10)) as root_scrap_percent,

    cast(substring(comp_item_uom, 1, 10) as text(10)) as comp_item_uom,

    cast(substring(root_item_uom, 1, 10) as text(10)) as root_item_uom,

    cast(substring(parent_item_uom, 1, 10) as text(10)) as parent_item_uom,

    cast(effective_date as date) as effective_date,

    cast(expiration_date as date) as expiration_date,

    cast(substring(active_flag, 1, 4) as text(4)) as active_flag,

    cast(bom_level as number(15, 0)) as bom_level,

    cast(substring(bom_path, 1, 1024) as text(1024)) as bom_path,

    cast(base_item_std_cost as number(38, 10)) as base_item_std_cost,

    cast(pcomp_item_std_cost as number(38, 10)) as pcomp_item_std_cost,

    cast(base_item_last_cost as number(38, 10)) as base_item_last_cost,

    cast(pcomp_item_last_cost as number(38, 10)) as pcomp_item_last_cost,

    cast(source_updated_date as date) as source_updated_date,

    cast(load_date as date) as load_date,

    cast(update_date as date) as update_date,

    cast(comp_perfection_qty as number(38, 10)) as comp_perfection_qty,

    cast(substring(item_model_group, 1, 255) as text(255)) as item_model_group,
    cast(
      {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "source_bom_identifier",
                        "bom_identifier_path",
                        "root_company_code",
                        "root_business_unit_code",
                        "active_flag",
                    ]
                )
            }} as text(255) ) as unique_key
from final
