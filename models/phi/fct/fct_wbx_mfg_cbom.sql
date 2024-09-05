{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["manufacturing", "cbom","mfg_cbom","sales","cogs","pcos"],
    unique_key="UNIQUE_KEY",
    on_schema_change='sync_all_columns', 
    pre_hook="""
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
        truncate table {{ this }}
    {% endif %}  """,
    )
}}

/* Significant adjustments required for the D365 project around May 2024.  Though there are 2 phases, BR1 for May 2024 and BR2 for Nov 2024, the changes for BR1 will 
    be intended to work for BR2 when the full set of Weetabix companies (IBE, RFL, and WBX) are to be included.  There may be some limits to what is possible or specified for WBX though
    until later during the BR2 work.

    Few actual changes will be applied in this, the fact model itself, as most transformation logic takes place in stg, int, and in many cases downstream from the fact model
    as this is used as a key input to other models that must calculate Cost of Goods (PCOS) for example.

    The CBOM will need to be handled differently depending on the Company in question (currently WBX, IBE, or RFL).
    -   Iberica (IBE) does not have the cost break down through the use of Cost Groups in AX/D365 the way the other companies can.  IBE only has a "BoughtIn" cost.
        The deployed code will reflect that requirement, essentially slotting the cost in to the BoughtIn bucket for PCOS.

*/
with
    int_fact as (
        select *,'D365' as source_legacy from {{ ref("int_f_wtx_mfg_cbom") }} qualify row_number() over(partition by unique_key order by 1)=1
    ),

/*
old_ax_fact as(
    select * from {{ ref("conv_fct_wbx_mfg_cbom") }}  ---commenting out conv model as this is a truncte load model,can be used if needed in future
),
*/

/*
ax_hist as(
select
a.source_system  ,
a.active_flag  ,
a.version_id  ,
a.eff_date  ,
a.creation_date_time  ,
a.expir_date  ,
a.source_updated_datetime  ,
a.transaction_currency  ,
a.transaction_uom  ,
a.root_company_code  ,
a.root_src_item_identifier  ,
a.root_src_variant_code  ,
a.root_src_item_guid  ,
a.comp_src_item_identifier  ,
a.comp_src_variant_code  ,
a.comp_src_item_guid  ,
a.comp_consumption_qty  ,
a.comp_consumption_unit  ,
a.comp_cost_price  ,
a.comp_cost_price_unit  ,
a.comp_item_unit_cost  ,
a.comp_bom_level  ,
a.comp_calctype_desc  ,
a.comp_cost_group_id  ,
a.parent_item_indicator  ,
a.source_business_unit_code  ,
a.source_bom_path  ,
a.stock_site  ,
a.root_src_unit_price  ,
a.unique_key,
a.source_legacy
 from old_ax_fact a 
 left join int_fact b on a.unique_key=b.unique_key where b.source_system is null
),
*/
final as(
    select * from int_fact
    /*union all
    select * from ax_hist*/  ---commenting out conv model as this is a truncte load model,can be used if needed in future
)
          
select
        cast(substring(source_system,1,255) as text(255) )              as source_system  ,
        cast(substring(active_flag,1,255) as text(255) )                as active_flag  ,
        cast(substring(version_id,1,255) as text(255) )                 as version_id  ,
        cast(eff_date as timestamp_ntz(9) )                             as eff_date  ,
        cast(creation_date_time as timestamp_ntz(9) )                   as creation_date_time  ,
        cast(expir_date as timestamp_ntz(9) )                           as expir_date  ,
        cast(source_updated_datetime as timestamp_ntz(9) )              as source_updated_datetime  ,
        cast(substring(transaction_currency,1,255) as text(255) )       as transaction_currency  ,
        cast(substring(transaction_uom,1,255) as text(255) )            as transaction_uom  ,
        cast(substring(root_company_code,1,255) as text(255) )          as root_company_code  ,
        cast(substring(root_src_item_identifier,1,255) as text(255) )   as root_src_item_identifier  ,
        cast(substring(root_src_variant_code,1,255) as text(255) )      as root_src_variant_code  ,
        cast(root_src_item_guid as text(255) )                          as root_src_item_guid  ,
        cast(substring(comp_src_item_identifier,1,255) as text(255) )   as comp_src_item_identifier  ,
        cast(substring(comp_src_variant_code,1,255) as text(255) )      as comp_src_variant_code  ,
        cast(comp_src_item_guid as text(255) )                          as comp_src_item_guid  ,
        cast(comp_consumption_qty as number(38,10) )                    as comp_consumption_qty  ,
        cast(comp_consumption_unit as number(38,10) )                   as comp_consumption_unit  ,
        cast(comp_cost_price as number(38,10) )                         as comp_cost_price  ,
        cast(comp_cost_price_unit as number(38,10) )                    as comp_cost_price_unit  ,
        cast(comp_item_unit_cost as number(38,10) )                     as comp_item_unit_cost  ,
        cast(comp_bom_level as number(38,0) )                           as comp_bom_level  ,
        cast(substring(comp_calctype_desc,1,255) as text(255) )         as comp_calctype_desc  ,
        cast(substring(comp_cost_group_id,1,255) as text(255) )         as comp_cost_group_id  ,
        cast(substring(parent_item_indicator,1,255) as text(255) )      as parent_item_indicator  ,
        cast(substring(source_business_unit_code,1,255) as text(255) )  as source_business_unit_code  ,
        cast(substring(source_bom_path,1,255) as text(255) )            as source_bom_path  ,
        cast(substring(stock_site,1,255) as text(255) )                 as stock_site  ,
        cast(root_src_unit_price as number(38,10) )                     as root_src_unit_price  ,
        cast(unique_key as text(255) )                                  as unique_key,
        cast(source_legacy as text(15) )                               as source_legacy
    from final