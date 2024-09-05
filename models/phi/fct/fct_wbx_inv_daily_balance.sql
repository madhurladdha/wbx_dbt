{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["inventory", "trans_ledger","inv_daily_balance","inv_aging"] ,
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    unique_key='INVENTORY_SNAPSHOT_DATE', 
    on_schema_change='sync_all_columns', 
    incremental_strategy='delete+insert',
    full_refresh=false,
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

with int_fact as (
    select *
    from {{ ref('int_f_wbx_inv_daily_balance') }} qualify
        row_number() over (partition by unique_key order by 1) = 1
),

/* Pulls the corresponding AX history across to blend w/ the D365 transactional data.
    For the conv model, the relevant dimension values have been converted, where that is applicable.
*/
old_ax_fact as (
    select * from {{ ref('conv_fct_wbx_inv_daily_balance') }}
),

int as (
    select
            inventory_snapshot_date,
             {{ dbt_utils.surrogate_key(["source_system","source_item_identifier"]) }}  as item_guid,
            source_item_identifier,
             {{ dbt_utils.surrogate_key(["source_system","SOURCE_BUSINESS_UNIT_CODE","'PLANT_DC'"]) }} as  business_unit_address_guid,
            source_business_unit_code,
            source_location_code,
            source_lot_code,
            primary_location_flag,
            lot_status_code,
            lot_status_desc,
            last_receipt_date,
            on_hand_qty,
            backorder_qty,
            purchase_order_qty,
            work_order_receipt_qty,
            hard_committed_qty,
            soft_committed_qty,
            future_commit_qty,
            wo_soft_commit_qty,
            wo_hard_commit_qty,
            in_transit_qty,
            in_inspection_qty,
            on_loan_qty,
            inbound_warehouse_qty,
            outbound_warehouse_qty,
            date_next_count,
            source_system,
            load_date,
            update_date,
            source_updated_d_id,
            {{ dbt_utils.surrogate_key(["SOURCE_SYSTEM","SOURCE_LOCATION_CODE","SOURCE_BUSINESS_UNIT_CODE"]) }}   as location_guid,
            {{ dbt_utils.surrogate_key(["SOURCE_SYSTEM","SOURCE_BUSINESS_UNIT_CODE","SOURCE_ITEM_IDENTIFIER","SOURCE_LOT_CODE"]) }}  as lot_guid,
            transaction_date,
            primary_uom,
            on_hand_kg_qty,
            on_hand_lb_qty,
            base_currency,
            phi_currency,
            pcomp_currency,
            oc_base_conv_rt,
            oc_corp_conv_rt,
            oc_pcomp_conv_rt,
            oc_base_item_unit_prim_cost,
            oc_corp_item_unit_prim_cost,
            oc_pcomp_item_unit_prim_cost,
            oc_base_on_hand_inv_prim_amt,
            oc_corp_on_hand_inv_prim_amt,
            oc_pcomp_on_hand_inv_prim_amt,
            best_by_date,
            monthstoexpire_6_date,
            on_hand_pl_qty,
            variant,
            snapshot_lot_expir_date,
            snapshot_lot_age_days,
            snapshot_lot_on_hand_date,
            snapshot_lot_sellby_date,
            unique_key,
            'D365' as source_legacy
        from int_fact
),


ax_hist as (
    select
            a.inventory_snapshot_date,
            a.item_guid,
            a.source_item_identifier,
            a.business_unit_address_guid,
            a.source_business_unit_code,
            a.source_location_code,
            a.source_lot_code,
            a.primary_location_flag,
            a.lot_status_code,
            a.lot_status_desc,
            a.last_receipt_date,
            a.on_hand_qty,
            a.backorder_qty,
            a.purchase_order_qty,
            a.work_order_receipt_qty,
            a.hard_committed_qty,
            a.soft_committed_qty,
            a.future_commit_qty,
            a.wo_soft_commit_qty,
            a.wo_hard_commit_qty,
            a.in_transit_qty,
            a.in_inspection_qty,
            a.on_loan_qty,
            a.inbound_warehouse_qty,
            a.outbound_warehouse_qty,
            a.date_next_count,
            a.source_system,
            a.load_date,
            a.update_date,
            a.source_updated_d_id,
            a.location_guid,
            a.lot_guid,
            a.transaction_date,
            a.primary_uom,
            a.on_hand_kg_qty,
            a.on_hand_lb_qty,
            a.base_currency,
            a.phi_currency,
            a.pcomp_currency,
            a.oc_base_conv_rt,
            a.oc_corp_conv_rt,
            a.oc_pcomp_conv_rt,
            a.oc_base_item_unit_prim_cost,
            a.oc_corp_item_unit_prim_cost,
            a.oc_pcomp_item_unit_prim_cost,
            a.oc_base_on_hand_inv_prim_amt,
            a.oc_corp_on_hand_inv_prim_amt,
            a.oc_pcomp_on_hand_inv_prim_amt,
            a.best_by_date,
            a.monthstoexpire_6_date,
            a.on_hand_pl_qty,
            a.variant,
            a.snapshot_lot_expir_date,
            a.snapshot_lot_age_days,
            a.snapshot_lot_on_hand_date,
            a.snapshot_lot_sellby_date,
            a.unique_key,
            'AX' as source_legacy
            from old_ax_fact as a
    left join int as b on a.unique_key = b.unique_key
    where b.source_system is null
), 

final as (
    select * from int
    union
    select * from ax_hist
)

select
        cast(inventory_snapshot_date as timestamp_ntz(9) ) as inventory_snapshot_date  ,
        cast(substring(item_guid,1,255) as text(255) ) as item_guid  ,
        cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
        cast(substring(business_unit_address_guid,1,255) as text(255) ) as business_unit_address_guid  ,
        cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
        cast(substring(source_location_code,1,255) as text(255) ) as source_location_code  ,
        cast(substring(source_lot_code,1,255) as text(255) ) as source_lot_code  ,
        cast(substring(primary_location_flag,1,1) as text(1) ) as primary_location_flag  ,
        cast(substring(lot_status_code,1,255) as text(255) ) as lot_status_code  ,
        cast(substring(lot_status_desc,1,30) as text(30) ) as lot_status_desc  ,
        cast(last_receipt_date as timestamp_ntz(9) ) as last_receipt_date  ,
        cast(on_hand_qty as number(15,2) ) as on_hand_qty  ,
        cast(backorder_qty as number(15,2) ) as backorder_qty  ,
        cast(purchase_order_qty as number(15,2) ) as purchase_order_qty  ,
        cast(work_order_receipt_qty as number(15,2) ) as work_order_receipt_qty  ,
        cast(hard_committed_qty as number(15,2) ) as hard_committed_qty  ,
        cast(soft_committed_qty as number(15,2) ) as soft_committed_qty  ,
        cast(future_commit_qty as number(15,2) ) as future_commit_qty  ,
        cast(wo_soft_commit_qty as number(15,2) ) as wo_soft_commit_qty  ,
        cast(wo_hard_commit_qty as number(15,2) ) as wo_hard_commit_qty  ,
        cast(in_transit_qty as number(15,2) ) as in_transit_qty  ,
        cast(in_inspection_qty as number(15,2) ) as in_inspection_qty  ,
        cast(on_loan_qty as number(15,2) ) as on_loan_qty  ,
        cast(inbound_warehouse_qty as number(15,2) ) as inbound_warehouse_qty  ,
        cast(outbound_warehouse_qty as number(15,2) ) as outbound_warehouse_qty  ,
        cast(date_next_count as timestamp_ntz(9) ) as date_next_count  ,
        cast(substring(source_system,1,255) as text(255) ) as source_system  ,
        cast(load_date as timestamp_ntz(9) ) as load_date  ,
        cast(update_date as timestamp_ntz(9) ) as update_date  ,
        cast(source_updated_d_id as number(15,0) ) as source_updated_d_id  ,
        cast(substring(location_guid,1,255) as text(255) ) as location_guid  ,
        cast(substring(lot_guid,1,255) as text(255) ) as lot_guid  ,
        cast(transaction_date as timestamp_ntz(9) ) as transaction_date  ,
        cast(substring(primary_uom,1,20) as text(20) ) as primary_uom  ,
        cast(on_hand_kg_qty as number(15,2) ) as on_hand_kg_qty  ,
        cast(on_hand_lb_qty as number(15,2) ) as on_hand_lb_qty  ,
        cast(substring(base_currency,1,20) as text(20) ) as base_currency  ,
        cast(substring(phi_currency,1,20) as text(20) ) as phi_currency  ,
        cast(substring(pcomp_currency,1,20) as text(20) ) as pcomp_currency  ,
        cast(oc_base_conv_rt as number(38,10) ) as oc_base_conv_rt  ,
        cast(oc_corp_conv_rt as number(38,10) ) as oc_corp_conv_rt  ,
        cast(oc_pcomp_conv_rt as number(38,10) ) as oc_pcomp_conv_rt  ,
        cast(oc_base_item_unit_prim_cost as number(38,10) ) as oc_base_item_unit_prim_cost  ,
        cast(oc_corp_item_unit_prim_cost as number(38,10) ) as oc_corp_item_unit_prim_cost  ,
        cast(oc_pcomp_item_unit_prim_cost as number(38,10) ) as oc_pcomp_item_unit_prim_cost  ,
        cast(oc_base_on_hand_inv_prim_amt as number(38,10) ) as oc_base_on_hand_inv_prim_amt  ,
        cast(oc_corp_on_hand_inv_prim_amt as number(38,10) ) as oc_corp_on_hand_inv_prim_amt  ,
        cast(oc_pcomp_on_hand_inv_prim_amt as number(38,10) ) as oc_pcomp_on_hand_inv_prim_amt  ,
        cast(best_by_date as timestamp_ntz(9) ) as best_by_date  ,
        cast(monthstoexpire_6_date as timestamp_ntz(9) ) as monthstoexpire_6_date  ,
        cast(on_hand_pl_qty as number(15,2) ) as on_hand_pl_qty  ,
        cast(substring(variant,1,255) as text(255) ) as variant  ,
        cast(snapshot_lot_expir_date as date) as snapshot_lot_expir_date  ,
        cast(snapshot_lot_age_days as number(38,10) ) as snapshot_lot_age_days  ,
        cast(snapshot_lot_on_hand_date as date) as snapshot_lot_on_hand_date  ,
        cast(snapshot_lot_sellby_date as date) as snapshot_lot_sellby_date  ,
        cast(substring(unique_key,1,255) as text(255) ) as unique_key,
        cast(source_legacy as text(255)) as source_legacy
from final