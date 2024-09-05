{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact"]
    )
}}



with
    old_fct as (
        select *
        from {{ source("WBX_PROD_FACT", "fct_wbx_mfg_cbom") }}
        where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
    ),

    old_plant as (
        select
           source_business_unit_code,
           source_business_unit_code_new
        from {{ ref('conv_dim_wbx_plant_dc') }}
    ),

converted_fct as (
select
source_system,
active_flag,
version_id,
eff_date,
creation_date_time,
expir_date,
source_updated_datetime,
transaction_currency,
transaction_uom,
root_company_code,
root_src_item_identifier,
root_src_variant_code,
a.root_src_item_guid as root_src_item_guid_old,
{{ dbt_utils.surrogate_key(
         ["a.source_system",
          "root_src_item_identifier",
          "plnt.source_business_unit_code_new"
         ])
        }} as root_src_item_guid,
comp_src_item_identifier,
comp_src_variant_code,
a.comp_src_item_guid as comp_src_item_guid_old,
{{ dbt_utils.surrogate_key(
         ["a.source_system",
          "a.comp_src_item_identifier",
          "plnt.source_business_unit_code_new"
         ])
        }} as comp_src_item_guid,
comp_consumption_qty,
comp_consumption_unit,
comp_cost_price,
comp_cost_price_unit,
comp_item_unit_cost,
comp_bom_level,
comp_calctype_desc,
comp_cost_group_id,
parent_item_indicator,
a.source_business_unit_code as source_business_unit_code_old,
plnt.source_business_unit_code_new as source_business_unit_code,
source_bom_path,
stock_site,
root_src_unit_price,
a.unique_key as unique_key_old,
        {{    dbt_utils.surrogate_key(
                ["source_system",
                    "creation_date_time",
                    "root_src_item_identifier",
                    "root_src_variant_code",
                    "comp_src_item_identifier",
                    "comp_src_variant_code",
                    "plnt.source_business_unit_code_new",
                    "source_bom_path",
                    "stock_site"
                ]
            )
        }}     as unique_key,
'AX' as source_legacy
from old_fct a
left join
    old_plant plnt
on a.source_business_unit_code=plnt.source_business_unit_code
)

select
        cast(substring(source_system, 1, 255) as text(255))
            as source_system,
        cast(substring(active_flag, 1, 255) as text(255))
            as active_flag,
        cast(substring(version_id, 1, 255) as text(255))
            as version_id,
        cast(eff_date as timestamp_ntz(9))
            as eff_date,
        cast(creation_date_time as timestamp_ntz(9))
            as creation_date_time,
        cast(expir_date as timestamp_ntz(9))
            as expir_date,
        cast(source_updated_datetime as timestamp_ntz(9))
            as source_updated_datetime,
        cast(substring(transaction_currency, 1, 255) as text(255))
            as transaction_currency,
        cast(substring(transaction_uom, 1, 255) as text(255))
            as transaction_uom,
        cast(substring(root_company_code, 1, 255) as text(255))
            as root_company_code,
        cast(substring(root_src_item_identifier, 1, 255) as text(255))
            as root_src_item_identifier,
        cast(substring(root_src_variant_code, 1, 255) as text(255))
            as root_src_variant_code,
        cast(root_src_item_guid_old as text(255))
            as root_src_item_guid_old,
        cast(root_src_item_guid as text(255))
            as root_src_item_guid,
        cast(substring(comp_src_item_identifier, 1, 255) as text(255))
            as comp_src_item_identifier,
        cast(substring(comp_src_variant_code, 1, 255) as text(255))
            as comp_src_variant_code,
        cast(comp_src_item_guid_old as text(255))
            as comp_src_item_guid_old,
        cast(comp_src_item_guid as text(255))
            as comp_src_item_guid,
        cast(comp_consumption_qty as number(38, 10))
            as comp_consumption_qty,
        cast(comp_consumption_unit as number(38, 10))
            as comp_consumption_unit,
        cast(comp_cost_price as number(38, 10))
            as comp_cost_price,
        cast(comp_cost_price_unit as number(38, 10))
            as comp_cost_price_unit,
        cast(comp_item_unit_cost as number(38, 10))
            as comp_item_unit_cost,
        cast(comp_bom_level as number(38, 0))
            as comp_bom_level,
        cast(substring(comp_calctype_desc, 1, 255) as text(255))
            as comp_calctype_desc,
        cast(substring(comp_cost_group_id, 1, 255) as text(255))
            as comp_cost_group_id,
        cast(substring(parent_item_indicator, 1, 255) as text(255))
            as parent_item_indicator,
        cast(substring(source_business_unit_code_old, 1, 255) as text(255))
            as source_business_unit_code_old,
        cast(substring(source_business_unit_code, 1, 255) as text(255))
            as source_business_unit_code,
        cast(substring(source_bom_path, 1, 255) as text(255))
            as source_bom_path,
        cast(substring(stock_site, 1, 255) as text(255))
            as stock_site,
        cast(root_src_unit_price as number(38, 10))
            as root_src_unit_price,
        cast(unique_key_old as text(255))
            as unique_key_old,
        cast(unique_key as text(255))
            as unique_key,
        cast(source_legacy as text(15))
            as source_legacy
        
from converted_fct
