{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_inventory"]
    )
}}

with
old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_inv_aging") }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
),

old_plant as (
    select
        source_business_unit_code_new,
        source_business_unit_code,
        plantdc_address_guid_new,
        plantdc_address_guid
    from {{ ref('conv_dim_wbx_plant_dc') }}
),
--rename to reflect new model naming convention
--add plant/item/account table
--join as normal

converted_fct as (
    select
        cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
        cast(a.inventory_snapshot_date as timestamp_ntz(9) ) as inventory_snapshot_date  ,
        cast(substring(a.item_guid,1,255) as text(255) ) as item_guid  ,
        cast(substring(a.source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
        cast(substring(plnt.plantdc_address_guid_new,1,255) as text(255) ) as business_unit_address_guid  ,
        cast(substring(plnt.plantdc_address_guid,1,255) as text(255) ) as business_unit_address_guid_old  ,
        cast(substring(plnt.source_business_unit_code_new,1,255) as text(255) ) as source_business_unit_code  ,
        cast(substring(plnt.source_business_unit_code,1,255) as text(255) ) as source_business_unit_code_old  ,
        cast(substring(a.source_lot_code,1,255) as text(255) ) as source_lot_code  ,
        cast(substring(a.lot_guid,1,255) as text(255) ) as lot_guid_old ,
        cast(substring({{
                                dbt_utils.surrogate_key(
                                    [
                                        "a.SOURCE_SYSTEM",
                                        "upper(trim(plnt.source_business_unit_code_new))",
                                        "a.SOURCE_ITEM_IDENTIFIER",
                                        "upper(a.source_lot_code)",
                                    ]
                                )
                        }}, 1, 255) as varchar(255)) as lot_guid,
        cast(substring(a.transaction_uom,1,20) as text(20) ) as transaction_uom  ,
        cast(a.on_hand_qty as number(27,2) ) as on_hand_qty  ,
        cast(a.transaction_date as timestamp_ntz(9) ) as transaction_date  ,
        cast(a.lot_expiration_date as timestamp_ntz(9) ) as lot_expiration_date  ,
        cast(a.shelf_life_days as number(38,10) ) as shelf_life_days  ,
        cast(a.manufactured_date as timestamp_ntz(9) ) as manufactured_date  ,
        cast(a.prod_age_in_weeks as number(38,0) ) as prod_age_in_weeks  ,
        cast(a.prod_age_in_days as number(38,0) ) as prod_age_in_days  ,
        cast(ship_to_guideline as number(38,10) ) as ship_to_guideline  ,
        cast(a.salvage_date as timestamp_ntz(9) ) as salvage_date  ,
        cast(a.weeks_left as number(38,0) ) as weeks_left  ,
        cast(substring(a.inventory_age_status,1,20) as text(20) ) as inventory_age_status  ,
        cast(a.load_date as timestamp_ntz(9) ) as load_date  ,
        cast(a.update_date as timestamp_ntz(9) ) as update_date  ,
        cast(a.source_updated_d_id as number(38,0) ) as source_updated_d_id  ,
        cast(a.on_hand_kg_qty as number(27,2) ) as on_hand_kg_qty,
        cast(substring(a.unique_key,1,255) as text(255) ) as unique_key_old,
        from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid      

    )      


select
    converted_fct.*,
    {{
                                dbt_utils.surrogate_key(
                                    [
                                        "SOURCE_SYSTEM",
                                        "INVENTORY_SNAPSHOT_DATE",
                                        "SOURCE_ITEM_IDENTIFIER",
                                        "SOURCE_BUSINESS_UNIT_CODE",
                                        "SOURCE_LOT_CODE"
                                    ]
                                )
                            }} as unique_key
from converted_fct