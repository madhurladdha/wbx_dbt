{{ 
    config(
        tags=["inventory", "trans_ledger","inv_daily_balance","inv_aging"] ,
        snowflake_warehouse= env_var("DBT_WBX_SF_WH")
 ) 
}}

with 
wtx_balance_date_range as (select * from {{ ref("src_wtx_balance_date_range") }}),
dim_date as (select * from {{ ref("src_dim_date") }}),
cte_date_range as 
( 
    select to_date(x.calendar_date) as calendar_date
    from dim_date x
    , wtx_balance_date_range y 
    where to_date(x.calendar_date) between nvl(y.from_date,to_date(convert_timezone('UTC',current_timestamp))) -1
    and nvl(y.to_date,to_date(convert_timezone('UTC',current_timestamp))) -1
),

fct_wbx_inv_trans_ledger as 
(
    select *,
    case    
        when substr(trim(source_item_identifier),1,1)='P' 
        then cast(case when trim(variant)='' then '-' else nvl(trim(variant),'-') end as varchar(255)) else '-' end as variant_lkp
    from {{ ref("fct_wbx_inv_trans_ledger") }}
),

dim_wbx_item as (select * from {{ ref("dim_wbx_item") }} ),
dim_wbx_lot as (select * ,to_date(lot_expiration_date) as best_by_date from {{ ref("dim_wbx_lot") }} ),
ref_effective_currency_dim as  (select * from {{ ref("src_ref_effective_currency_dim") }}),
item_cost_dim  as (select * from {{ ref("dim_wbx_inv_item_cost") }} where dbt_valid_to is null and source_cost_method_code='07'),


  
trans_on_hand_pl as 
(
    select 
        a.item_guid as item_guid
        , a.business_unit_address_guid as business_unit_address_guid
        , a.location_guid as location_guid
        , a.lot_guid as lot_guid
        , a.variant as variant
        , b.calendar_date as calendar_date
        , sum(nvl(a.pallet_count,0)) as on_hand_pl_qty 
    from fct_wbx_inv_trans_ledger a
    , cte_date_range b 
    where 
        to_date(a.transaction_date) <= to_date(b.calendar_date)
        and a.document_type in ('INVRCPT','INVCOUNT','INVTRX','INVADJ')
    group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid, a.variant, b.calendar_date
    having sum(nvl(a.pallet_count,0)) > 0
) ,

trans_on_hand as 
(
    select 
        a.item_guid as item_guid
        , a.business_unit_address_guid as business_unit_address_guid
        , a.location_guid as location_guid
        , a.lot_guid as lot_guid
        , a.variant as variant
        , b.calendar_date as calendar_date
        , sum(a.transaction_pri_uom_qty) as transaction_qty_on_hand
        , sum(a.base_amt) as lookup_amt_on_hand
        , sum(a.transaction_kg_qty) as transaction_kg_qty_on_hand
    from fct_wbx_inv_trans_ledger a
    , cte_date_range b 
    where 
        to_date(a.transaction_date) <= to_date(b.calendar_date)
        and (a.transaction_qty <> 0 or a.transaction_amt <> 0)
        and a.document_type in ('INVRCPT','INVCOUNT','INVTRX','INVADJ')
    group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid, a.variant, b.calendar_date
    having sum(a.transaction_qty)<>0 or sum(a.transaction_amt)<>0
) ,

trans_for_period as 
(
    select 
        a.item_guid as item_guid 
        , a.business_unit_address_guid as business_unit_address_guid
        , a.location_guid as location_guid
        , a.lot_guid  as lot_guid
        , a.variant as variant
        , to_date(b.calendar_date) as calendar_date
        , sum(a.transaction_pri_uom_qty) as transaction_qty_po
        , sum(a.base_amt) as lookup_amt_po
        , sum(a.transaction_kg_qty) as transaction_kg_qty_po
    from fct_wbx_inv_trans_ledger a
    , cte_date_range b 
    where 
        to_date(a.transaction_date) = to_date(b.calendar_date)
        and a.source_document_type in ('3.0.1','3.0.2','3.0.3','3.1.0','3.2.0')
        and (a.transaction_qty <> 0 or a.transaction_amt <> 0)
    group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid, a.variant, b.calendar_date
    having sum(a.transaction_qty)<>0 or sum(a.transaction_amt)<>0
),     
                

joined_source as (
    select distinct  
        to_date(convert_timezone('UTC',current_timestamp)) as inventory_snapshot_date_tgt,
        cast(to_date(b.calendar_date) as timestamp_ntz(9) )  as inventory_snapshot_date,
        a.item_guid as item_guid,
        a.source_item_identifier as source_item_identifier,
        a.business_unit_address_guid as business_unit_address_guid,
        a.source_business_unit_code as source_business_unit_code,
        a.source_location_code as source_location_code,
        nvl(a.source_lot_code,'-') as source_lot_code,
        a.variant as variant,
        '-' as primary_location_flag,
        lm.lot_status_code as lot_status_code,
        lm.lot_status_desc as lot_status_desc,
        cast(null as string(255)) as last_receipt_date,
        cast(null as string(255)) as date_next_count,
        a.source_system as source_system,
        current_date as load_date,
        current_date as update_date,
        dim_dt.calendar_date_id as source_updated_d_id,
        a.location_guid as location_guid,
        a.lot_guid as lot_guid,
        cast(null as string(255)) as  transaction_date,
        im.primary_uom as primary_uom,
        lm.best_by_date,
        --case when substr(trim(a.source_item_identifier),1,1)='p' then cast(case when trim(a.variant)='' then '-' else nvl(trim(a.variant),'-') end as varchar(255)) else '-' end as variant_lkp,
        a.variant_lkp,
        lot_expiration_date as snapshot_lot_expir_date,
        lot_age_days as snapshot_lot_age_days,
        lot_onhand_date as snapshot_lot_on_hand_date,
        lot_sellby_date as snapshot_lot_sellby_date,
        nvl(trans_for_period.transaction_qty_po,0) as purchase_order_qty,
        trans_for_period.lookup_amt_po as lookup_amt_po,
        trans_for_period.transaction_kg_qty_po as transaction_kg_qty_po,
        nvl(trans_on_hand.transaction_qty_on_hand,0) as on_hand_qty,
        trans_on_hand.lookup_amt_on_hand as lookup_amt_on_hand,
        nvl(trans_on_hand.transaction_kg_qty_on_hand,0) as on_hand_kg_qty,
        nvl(trans_on_hand.transaction_kg_qty_on_hand*{{ent_dbt_package.lkp_constants("KG_LB_CONVERSION_RATE")}},0)on_hand_lb_qty,
        nvl(trans_on_hand_pl.on_hand_pl_qty,0) as on_hand_pl_qty,
        0 as backorder_qty ,
        0 as work_order_receipt_qty,
        0 as hard_committed_qty,
        0 as soft_committed_qty,
        0 as wo_soft_commit_qty,
        0 as wo_hard_commit_qty,
        0 as in_transit_qty,
        0 as in_inspection_qty,
        0 as on_loan_qty,
        0 as inbound_warehouse_qty,
        0 as outbound_warehouse_qty,
        'USD' as phi_currency,
        curr_dim.company_default_currency_code as base_currency,
        curr_dim.parent_currency_code as pcomp_currency,
        0 as future_commit_qty,
        nvl(item_cost_dim.oc_base_conv_rt,0) as oc_base_conv_rt,
        nvl(item_cost_dim.oc_corp_conv_rt,0) as oc_corp_conv_rt,
        nvl(item_cost_dim.oc_pcomp_conv_rt,0) as oc_pcomp_conv_rt,
        nvl(item_cost_dim.oc_base_item_unit_prim_cost,0) as oc_base_item_unit_prim_cost,
        nvl(item_cost_dim.oc_corp_item_unit_prim_cost,0) as oc_corp_item_unit_prim_cost,
        nvl(item_cost_dim.oc_pcomp_item_unit_prim_cost,0) as oc_pcomp_item_unit_prim_cost,
        nvl(item_cost_dim.variant_code,'-') as variant_code,
        nvl(item_cost_dim.oc_base_item_unit_prim_cost * trans_on_hand.transaction_qty_on_hand,0) as oc_base_on_hand_inv_prim_amt,
        nvl(item_cost_dim.oc_corp_item_unit_prim_cost * trans_on_hand.transaction_qty_on_hand,0) as oc_corp_on_hand_inv_prim_amt,
        nvl(item_cost_dim.oc_pcomp_item_unit_prim_cost * trans_on_hand.transaction_qty_on_hand,0) as oc_pcomp_on_hand_inv_prim_amt,
        add_months(lm.best_by_date, -6) as monthstoexpire_6_date
    from fct_wbx_inv_trans_ledger a
    inner join cte_date_range b 
        on to_date(a.transaction_date) <= to_date(b.calendar_date)
    left outer join  dim_wbx_item  im
        on im.item_guid = a.item_guid
        and im.business_unit_address_guid = a.business_unit_address_guid
    left outer join dim_wbx_lot  lm
        on lm.item_guid = a.item_guid
        and lm.business_unit_address_guid = a.business_unit_address_guid
        and lm.lot_guid = a.lot_guid
    left outer join  trans_for_period
        on trans_for_period.item_guid = a.item_guid
        and trans_for_period.business_unit_address_guid = a.business_unit_address_guid
        and trans_for_period.location_guid = a.location_guid
        and trans_for_period.lot_guid = a.lot_guid
        and trans_for_period.calendar_date = to_date(b.calendar_date)
        and trans_for_period.variant = a.variant
    left outer join trans_on_hand trans_on_hand
        on trans_on_hand.item_guid = a.item_guid
        and trans_on_hand.business_unit_address_guid = a.business_unit_address_guid
        and trans_on_hand.location_guid = a.location_guid
        and trans_on_hand.lot_guid = a.lot_guid
        and trans_on_hand.calendar_date = to_date(b.calendar_date)
        and trans_on_hand.variant = a.variant
    left outer join  trans_on_hand_pl trans_on_hand_pl
        on trans_on_hand_pl.item_guid = a.item_guid
        and trans_on_hand_pl.business_unit_address_guid = a.business_unit_address_guid
        and trans_on_hand_pl.location_guid = a.location_guid
        and trans_on_hand_pl.lot_guid = a.lot_guid
        and trans_on_hand_pl.calendar_date = to_date(b.calendar_date)
        and trans_on_hand_pl.variant = a.variant 
    left outer join dim_date dim_dt
        on dim_dt.calendar_date = current_date
    left outer join ref_effective_currency_dim  as curr_dim
        on curr_dim.source_system  = a.source_system
        and curr_dim.source_business_unit_code = a.source_business_unit_code
        and curr_dim.effective_date <= to_date(b.calendar_date)
        and curr_dim.expiration_date >= to_date(b.calendar_date)
    left outer join item_cost_dim item_cost_dim
        on item_cost_dim.source_system	=a.source_system	 
        and item_cost_dim.item_guid	=	a.item_guid	 
        and item_cost_dim.business_unit_address_guid=a.business_unit_address_guid	 
        and item_cost_dim.variant_code=	a.variant_lkp
    where nvl(a.source_lot_code,'-') <>'-'
) ,

target as (
    select 
    cast(inventory_snapshot_date_tgt as timestamp_ntz(9)) as inventory_snapshot_date  , -- set to the current date rather than the previous
    cast(item_guid as text(255) ) as item_guid  ,
    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
    cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,
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
    cast(location_guid as text(255) ) as location_guid  ,
    cast(lot_guid as text(255) ) as lot_guid  ,
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
    cast(snapshot_lot_sellby_date as date) as snapshot_lot_sellby_date  
    /* 
    cast({{ dbt_utils.surrogate_key(
            ["source_item_identifier","inventory_snapshot_date","source_business_unit_code","source_location_code","source_lot_code","variant"])
            }}  as text(255)) as unique_key
            */
	from joined_source
    where on_hand_qty !=0 or purchase_order_qty !=0 or on_hand_kg_qty !=0 or on_hand_lb_qty !=0 )

select *,
    cast({{
        dbt_utils.surrogate_key(
            ["SOURCE_ITEM_IDENTIFIER","INVENTORY_SNAPSHOT_DATE","SOURCE_BUSINESS_UNIT_CODE","SOURCE_LOCATION_CODE","SOURCE_LOT_CODE","VARIANT"]
        )
    }}  as text(255)) as unique_key
from target