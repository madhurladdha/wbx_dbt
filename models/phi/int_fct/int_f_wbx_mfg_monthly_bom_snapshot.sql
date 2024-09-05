{{
    config(
        tags=["wbx", "manufacturing", "bom", "work order"]
    )
}}

with
    dim_wbx_mfg_wo_item_bom as (
        select *, row_number() over (partition by unique_key order by 1) rownum
        from {{ ref("dim_wbx_mfg_wo_item_bom") }}
    ),

   cte_non_mnth_end as (select
            to_date(convert_timezone('UTC', current_timestamp)) as snapshot_date,
            trunc(
                to_date(convert_timezone('UTC', current_timestamp)), 'MONTH'
            ) as mnth_effective_date,
            to_date(convert_timezone('UTC', current_timestamp)) as mnth_expiration_date,
            source_system,
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
            active_flag,
            bom_level,
            bom_path,
            base_item_std_cost,
            pcomp_item_std_cost,
            base_item_last_cost,
            pcomp_item_last_cost,
            source_updated_date,
            comp_perfection_qty,
            load_date,
            update_date,
            'D' as bom_flag,
            item_model_group
        from dim_wbx_mfg_wo_item_bom
        where
            to_date(convert_timezone('UTC', current_timestamp))
            <> last_day((convert_timezone('UTC', current_timestamp)), 'MONTH')),

cte_mnth_end as (select
            to_date(convert_timezone('UTC', current_timestamp)) as snapshot_date,
            trunc(
                to_date(convert_timezone('UTC', current_timestamp)), 'MONTH'
            ) as mnth_effective_date,
            last_day(
                (convert_timezone('UTC', current_timestamp)), 'MONTH'
            ) as mnth_expiration_date,
            source_system,
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
            active_flag,
            bom_level,
            bom_path,
            base_item_std_cost,
            pcomp_item_std_cost,
            base_item_last_cost,
            pcomp_item_last_cost,
            source_updated_date,
            comp_perfection_qty,
            load_date,
            update_date,
            'M' bom_flag,
            item_model_group
        from dim_wbx_mfg_wo_item_bom
        where
            to_date(convert_timezone('UTC', current_timestamp))
            = last_day((convert_timezone('UTC', current_timestamp)), 'MONTH'))    ,        

    final as (
       select * from cte_non_mnth_end
        union
      select * from  cte_mnth_end
        
    )

    select * from final
