{{ config( 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  tags=["inventory", "trans_ledger","inv_daily_balance","inv_aging"]
) }}
with inv_daily_balance_fact as (
    select * from {{ ref('fct_wbx_inv_daily_balance')}}
),
item_master as (
    select * from {{ ref('dim_wbx_item')}}
),
lot_master as (
    select * from {{ ref('dim_wbx_lot')}}
),
plant_dc as (
    select * from {{ ref('dim_wbx_plant_dc')}}
),
final as (
    select
        f.inventory_snapshot_date,
        f.item_guid,
        f.source_item_identifier,
        f.business_unit_address_guid,
        f.source_business_unit_code,
        f.source_location_code,
        f.source_lot_code,
        f.variant,
        f.primary_location_flag,
        f.lot_status_code,
        f.lot_status_desc,
        f.last_receipt_date,
        f.on_hand_qty,
        f.backorder_qty,
        f.purchase_order_qty,
        f.work_order_receipt_qty,
        f.hard_committed_qty,
        f.soft_committed_qty,
        f.future_commit_qty,
        f.wo_soft_commit_qty,
        f.wo_hard_commit_qty,
        f.in_transit_qty,
        f.in_inspection_qty,
        f.on_loan_qty,
        f.inbound_warehouse_qty,
        f.outbound_warehouse_qty,
        0 as on_hold_qty,
        0 as expired_qty,
        0 as damaged_qty,
        f.date_next_count,
        f.source_system,
        f.load_date,
        f.update_date,
        f.source_updated_d_id,
        f.location_guid,
        f.lot_guid,
        f.transaction_date,
        f.primary_uom,
        f.on_hand_kg_qty,
        f.on_hand_lb_qty,
        f.oc_base_item_unit_prim_cost,
        f.oc_corp_item_unit_prim_cost,
        f.oc_pcomp_item_unit_prim_cost,
        f.oc_base_on_hand_inv_prim_amt,
        f.oc_corp_on_hand_inv_prim_amt,
        f.oc_pcomp_on_hand_inv_prim_amt,
        f.best_by_date,
        f.monthstoexpire_6_date,
        nvl(snapshot_lot_expir_date, l.lot_expiration_date) as lot_expiration_date,
        nvl(snapshot_lot_age_days, l.lot_age_days)          as lot_age_days,
        nvl(snapshot_lot_on_hand_date, l.lot_onhand_date)   as lot_onhand_date,
        nvl(snapshot_lot_sellby_date, l.lot_sellby_date)    as lot_sellby_date,
        case
            when d.item_type = 'PACKAGING'
            then datediff(
                 day,
                 to_date(to_char(nvl(snapshot_lot_on_hand_date, l.lot_onhand_date))),
                 to_date(to_char(nvl(snapshot_lot_sellby_date, l.lot_sellby_date)))
                ) + 1 else d.shelf_life_days end as shelf_life_days,
        d.item_class,
       -- d.case_upc,
        null as case_upc,
        d.case_item_number,
        d.description,
        d.stock_type,
        d.item_type,
        d.buyer_code,
        d1.business_unit_name,
        0 as demand_qty,
        'ON-HAND' as record_type,
        f.on_hand_pl_qty as on_hand_pl_qty
    from inv_daily_balance_fact f
    left join
        item_master d
        on f.item_guid = d.item_guid
        and f.business_unit_address_guid = d.business_unit_address_guid
        and f.source_system = d.source_system
    left join
        lot_master l
        on f.item_guid = l.item_guid
        and f.lot_guid = l.lot_guid
        and f.source_system = l.source_system
    left join
        plant_dc d1
        on f.business_unit_address_guid = d1.plantdc_address_guid
        and f.source_system = d1.source_system
    /*left join
        ei_rdm.uom_factor uom
        on uom.source_system = 'WEETABIX'
        and 'PL' = uom.to_uom
        and 'KG' = uom.from_uom
        and 'Y' = uom.active_flag
        and f.item_guid = uom.item_guid*/
    left join
    {{
        ent_dbt_package.lkp_uom("f.item_guid","'PL'","'KG'","uom",)
	}}
)
select
    inventory_snapshot_date,
	item_guid,
	source_item_identifier,
	business_unit_address_guid,
	source_business_unit_code,
	source_location_code,
	source_lot_code,
	variant,
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
	on_hold_qty,
	expired_qty,
	damaged_qty,
	date_next_count,
	source_system,
	load_date,
	update_date,
	source_updated_d_id,
	location_guid,
	lot_guid,
	transaction_date,
	primary_uom,
	on_hand_kg_qty,
	on_hand_lb_qty,
	oc_base_item_unit_prim_cost,
	oc_corp_item_unit_prim_cost,
	oc_pcomp_item_unit_prim_cost,
	oc_base_on_hand_inv_prim_amt,
	oc_corp_on_hand_inv_prim_amt,
	oc_pcomp_on_hand_inv_prim_amt,
	best_by_date,
	monthstoexpire_6_date,
	lot_expiration_date,
	lot_age_days,
	lot_onhand_date,
	lot_sellby_date,
	shelf_life_days,
	item_class,
	case_upc,
	case_item_number,
	description,
	stock_type,
	item_type,
	buyer_code,
	business_unit_name,
	demand_qty,
	record_type,
	on_hand_pl_qty
from final
