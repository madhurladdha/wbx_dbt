{{ config( 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  tags=["manufacturing", "supply_Schedule", "wbx", "weekly", "inventory"],

) }}
with mfg_supply_sched as (
    select * from {{ ref('fct_wbx_mfg_supply_sched_wkly_agg')}}
),
item_master as (
    select
        source_item_identifier,
        source_system,
        max(primary_uom) primary_uom,
        max(item_type) item_type,
        max(description) description,
        max(buyer_code) buyer_code,
        max(vendor_address_guid) vendor_address_guid
    from {{ ref('dim_wbx_item')}}
    where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    group by source_item_identifier, source_system
),
plant_dc as (
    select * from {{ ref('dim_wbx_plant_dc')}}
),
supplier as (
    select * from {{ ref('dim_wbx_supplier')}}
),
/*item_variant as ( -- avoided use of cte due to dbt limitation. 
Getting SF error Processing aborted due to error 300002:1707660727; incident 2468834. while using cte
    select * from {{ ref('dim_wbx_mfg_item_variant')}}
),*/
item_variant as (
    select
        s.source_system,
        s.source_item_identifier,
        trim(s.variant_code) as variant_code,
        s.active_flag,
        max(s.variant_desc) variant_desc,
        max(s.item_allocation_key) item_allocation_key
    from
        {{ ref('dim_wbx_mfg_item_variant')}} s,
        (
    select
        source_system,
        source_item_identifier,
        trim(variant_code) as variant_code,
        max(active_flag) active_flag
    from {{ ref('dim_wbx_mfg_item_variant')}}
        group by source_system, source_item_identifier, trim(variant_code)
    ) d
    where
        s.source_item_identifier = d.source_item_identifier
        and trim(s.variant_code) = trim(d.variant_code)
        and s.source_system = d.source_system
        and s.active_flag = d.active_flag
        group by
            s.source_system,
            s.source_item_identifier,
            trim(s.variant_code),
            s.active_flag
),

final as (
    select
        a.source_system,
        snapshot_date,
        a.source_business_unit_code,
        a.source_item_identifier,
        d.item_type,
        c.item_allocation_key,
        a.variant_code,
        source_company_code,
        source_site_code,
        plan_version,
        a.item_guid,
        a.business_unit_address_guid,
        week_desc,
        week_start_dt,
        week_end_dt,
        current_week_flag,
        week_start_stock,
        demand_transit_qty,
        supply_transit_qty,
        expired_qty,
        blocked_qty,
        demand_wo_qty,
        production_wo_qty,
        production_planned_wo_qty,
        demand_planned_batch_wo_qty,
        prod_planned_batch_wo_qty,
        supply_trans_journal_qty,
        demand_trans_journal_qty,
        demand_planned_trans_qty,
        supply_stock_journal_qty,
        demand_stock_journal_qty,
        supply_po_qty,
        supply_planned_po_qty,
        supply_po_transfer_qty,
        supply_po_return_qty,
        sales_order_qty,
        return_sales_order_qty,
        minimum_stock_qty,
        week_end_stock,
        (case when (c.variant_desc is null or trim(c.variant_desc) = '')
              then to_char(d.description)
              else to_char(c.variant_desc) end) as description,
        d.buyer_code,
        d.vendor_address_guid,
        e.supplier_name,
        period_end_firm_purchase_qty,
        a.demand_unplanned_batch_wo_qty,
        (   supply_transit_qty
            + supply_trans_journal_qty
            + supply_stock_journal_qty
            + supply_po_qty
            + supply_planned_po_qty
            + supply_po_transfer_qty
            + return_sales_order_qty) as total_supply_qty,

        (   demand_transit_qty
            + demand_wo_qty
            + demand_planned_batch_wo_qty
            + demand_trans_journal_qty
            + demand_planned_trans_qty
            + demand_stock_journal_qty
            + supply_po_return_qty ) as total_demand_qty,
        primary_uom

    from mfg_supply_sched a
    inner join item_master d 
        on  a.source_item_identifier = d.source_item_identifier
        and a.source_system = d.source_system
    inner join plant_dc b on
        a.source_business_unit_code = b.source_business_unit_code
        and a.source_system = b.source_system
    left join supplier e on
        e.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
        and cast(ltrim(e.source_system_address_number, '0') as varchar2(255))
        = cast(trunc(d.vendor_address_guid) as varchar2(255))
        and UPPER(TRIM(e.company_code)) = UPPER(TRIM(a.source_company_code))
    left join item_variant c
        on a.source_system = c.source_system
        and a.source_item_identifier = c.source_item_identifier
        and trim(a.variant_code) = trim(c.variant_code)
)
select 
    source_system,
	snapshot_date,
	source_business_unit_code,
	source_item_identifier,
	item_type,
	item_allocation_key,
	variant_code,
	source_company_code,
	source_site_code,
	plan_version,
	item_guid,
	business_unit_address_guid,
	week_desc,
	week_start_dt,
	week_end_dt,
	current_week_flag,
	week_start_stock,
	demand_transit_qty,
	supply_transit_qty,
	expired_qty,
	blocked_qty,
	demand_wo_qty,
	production_wo_qty,
	production_planned_wo_qty,
	demand_planned_batch_wo_qty,
	prod_planned_batch_wo_qty,
	supply_trans_journal_qty,
	demand_trans_journal_qty,
	demand_planned_trans_qty,
	supply_stock_journal_qty,
	demand_stock_journal_qty,
	supply_po_qty,
	supply_planned_po_qty,
	supply_po_transfer_qty,
	supply_po_return_qty,
	sales_order_qty,
	return_sales_order_qty,
	minimum_stock_qty,
	week_end_stock,
	description,
	buyer_code,
	vendor_address_guid,
	supplier_name,
	period_end_firm_purchase_qty,
	demand_unplanned_batch_wo_qty,
	total_supply_qty,
	total_demand_qty,
	primary_uom
from final
