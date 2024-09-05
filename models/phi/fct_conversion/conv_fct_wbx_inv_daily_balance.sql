{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_inventory"]
    )
}}

with
old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_inv_daily_balance") }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
), --make sure this flag is set to yes, this allows the system to pull the history. Without this, history will not come through.

--cross references from history that we will need to convert from old to new
/* For AR Customer Invoice Header, it appears as though the source_business_unit_code (plant) is always NULL.
    This means that we can do this "conversion" of plant, but there is not impact since all are NULL.
*/
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
        cast(a.inventory_snapshot_date as timestamp_ntz(9) ) as inventory_snapshot_date  ,
        cast(item_guid as text(255)) as item_guid,
        cast(substring(a.source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
        cast(substring(plnt.plantdc_address_guid_new,1,255) as text(255) ) as business_unit_address_guid  ,
        cast(substring(plnt.plantdc_address_guid,1,255) as text(255) ) as business_unit_address_guid_old  ,
        cast(substring(plnt.source_business_unit_code_new,1,255) as text(255) ) as source_business_unit_code  ,
        cast(substring(plnt.source_business_unit_code,1,255) as text(255) ) as source_business_unit_code_old  ,
        cast(substring(a.source_location_code,1,255) as text(255) ) as source_location_code  ,
        cast(substring(a.source_lot_code,1,255) as text(255) ) as source_lot_code  ,
        cast(substring(a.primary_location_flag,1,1) as text(1) ) as primary_location_flag  ,
        cast(substring(a.lot_status_code,1,255) as text(255) ) as lot_status_code  ,
        cast(substring(a.lot_status_desc,1,30) as text(30) ) as lot_status_desc  ,
        cast(a.last_receipt_date as timestamp_ntz(9) ) as last_receipt_date  ,
        cast(a.on_hand_qty as number(15,2) ) as on_hand_qty  ,
        cast(a.backorder_qty as number(15,2) ) as backorder_qty  ,
        cast(a.purchase_order_qty as number(15,2) ) as purchase_order_qty  ,
        cast(a.work_order_receipt_qty as number(15,2) ) as work_order_receipt_qty  ,
        cast(a.hard_committed_qty as number(15,2) ) as hard_committed_qty  ,
        cast(a.soft_committed_qty as number(15,2) ) as soft_committed_qty  ,
        cast(a.future_commit_qty as number(15,2) ) as future_commit_qty  ,
        cast(a.wo_soft_commit_qty as number(15,2) ) as wo_soft_commit_qty  ,
        cast(a.wo_hard_commit_qty as number(15,2) ) as wo_hard_commit_qty  ,
        cast(a.in_transit_qty as number(15,2) ) as in_transit_qty  ,
        cast(a.in_inspection_qty as number(15,2) ) as in_inspection_qty  ,
        cast(a.on_loan_qty as number(15,2) ) as on_loan_qty  ,
        cast(a.inbound_warehouse_qty as number(15,2) ) as inbound_warehouse_qty  ,
        cast(a.outbound_warehouse_qty as number(15,2) ) as outbound_warehouse_qty  ,
        cast(a.date_next_count as timestamp_ntz(9) ) as date_next_count  ,
        cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
        cast(a.load_date as timestamp_ntz(9) ) as load_date  ,
        cast(a.update_date as timestamp_ntz(9) ) as update_date  ,
        cast(a.source_updated_d_id as number(15,0) ) as source_updated_d_id  ,
        cast(substring(a.location_guid,1,255) as text(255) ) as location_guid_old  ,
        cast(substring({{ dbt_utils.surrogate_key
                               (
                                    [
                                        "a.SOURCE_SYSTEM",
                                        "upper(a.SOURCE_LOCATION_CODE)",
                                        "upper(trim(plnt.source_business_unit_code_new))",
                                    ]
                                )
                        }}, 1, 255) as varchar(255)) as location_guid,
        cast(substring(a.lot_guid,1,255) as text(255) ) as lot_guid_old  ,
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
        cast(a.transaction_date as timestamp_ntz(9) ) as transaction_date  ,
        cast(substring(a.primary_uom,1,20) as text(20) ) as primary_uom  ,
        cast(a.on_hand_kg_qty as number(15,2) ) as on_hand_kg_qty  ,
        cast(a.on_hand_lb_qty as number(15,2) ) as on_hand_lb_qty  ,
        cast(substring(a.base_currency,1,20) as text(20) ) as base_currency  ,
        cast(substring(a.phi_currency,1,20) as text(20) ) as phi_currency  ,
        cast(substring(a.pcomp_currency,1,20) as text(20) ) as pcomp_currency  ,
        cast(a.oc_base_conv_rt as number(38,10) ) as oc_base_conv_rt  ,
        cast(a.oc_corp_conv_rt as number(38,10) ) as oc_corp_conv_rt  ,
        cast(a.oc_pcomp_conv_rt as number(38,10) ) as oc_pcomp_conv_rt  ,
        cast(a.oc_base_item_unit_prim_cost as number(38,10) ) as oc_base_item_unit_prim_cost  ,
        cast(a.oc_corp_item_unit_prim_cost as number(38,10) ) as oc_corp_item_unit_prim_cost  ,
        cast(a.oc_pcomp_item_unit_prim_cost as number(38,10) ) as oc_pcomp_item_unit_prim_cost  ,
        cast(a.oc_base_on_hand_inv_prim_amt as number(38,10) ) as oc_base_on_hand_inv_prim_amt  ,
        cast(a.oc_corp_on_hand_inv_prim_amt as number(38,10) ) as oc_corp_on_hand_inv_prim_amt  ,
        cast(a.oc_pcomp_on_hand_inv_prim_amt as number(38,10) ) as oc_pcomp_on_hand_inv_prim_amt  ,
        cast(a.best_by_date as timestamp_ntz(9) ) as best_by_date  ,
        cast(a.monthstoexpire_6_date as timestamp_ntz(9) ) as monthstoexpire_6_date  ,
        cast(a.on_hand_pl_qty as number(15,2) ) as on_hand_pl_qty  ,
        cast(substring(a.variant,1,255) as text(255) ) as variant  ,
        cast(a.snapshot_lot_expir_date as date) as snapshot_lot_expir_date  ,
        cast(a.snapshot_lot_age_days as number(38,10) ) as snapshot_lot_age_days  ,
        cast(a.snapshot_lot_on_hand_date as date) as snapshot_lot_on_hand_date  ,
        cast(a.snapshot_lot_sellby_date as date) as snapshot_lot_sellby_date,
        cast(a.unique_key as text(255)) as unique_key_old,

        from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid      

    )

select
    converted_fct.*,
    {{
        dbt_utils.surrogate_key(
            ["SOURCE_ITEM_IDENTIFIER","INVENTORY_SNAPSHOT_DATE","SOURCE_BUSINESS_UNIT_CODE","SOURCE_LOCATION_CODE","SOURCE_LOT_CODE","VARIANT"]
        )
    }} as unique_key
from converted_fct