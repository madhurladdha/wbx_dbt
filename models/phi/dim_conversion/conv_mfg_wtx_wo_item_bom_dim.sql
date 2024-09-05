

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'mfg_wtx_wo_item_bom_dim') }}

),

renamed as (

    select
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
            }} as unique_key

    from source

)

select * from renamed where {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
