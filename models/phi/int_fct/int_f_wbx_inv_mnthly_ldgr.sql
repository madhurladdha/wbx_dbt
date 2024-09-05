{{ config(tags=["inventory", "inv_monthly_ledger"] ,
        materialized=env_var('DBT_MAT_TABLE'),
          transient=true,
          snowflake_warehouse= env_var("DBT_WBX_SF_WH")
 ) }}

with
    source_details as (select * from {{ ref("fct_wbx_inv_trans_ledger") }}),
    dim_date as (select * from {{ ref("src_dim_date") }}),

    date_range as
    (
        select distinct fiscal_period_begin_dt, fiscal_period_end_dt, fiscal_year_period_no, calendar_year_month_no, fiscal_period_no
        from dim_date where calendar_date between '01-OCT-2015' and to_date(current_date)
    ),


    lkp_inv_trans_for_period_ledger as (
        select 
            a.item_guid as item_guid
            , a.business_unit_address_guid as business_unit_address_guid
            , a.location_guid as location_guid
            , a.lot_guid as lot_guid
            , b.fiscal_year_period_no as fiscal_year_period_no
            , sum(a.transaction_pri_uom_qty) as transaction_qty
            , sum(a.transaction_kg_qty)  as transaction_kg_qty
            , sum(a.base_amt) as lookup_amt
        from fct_wbx_inv_trans_ledger a
        , dim_date b
        where a.transaction_date = b.calendar_date
            and (a.transaction_qty <> 0 or a.transaction_amt <> 0)
            and a.document_type in ('INVRCPT','INVCOUNT','INVTRX')
            and a.gl_date is not null and a.gl_date <> '01-JAN-1900'
        group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid, b.fiscal_year_period_no
        having sum(a.transaction_qty)<>0 or sum(a.transaction_amt)<>0) ,

lkp_inv_trans_for_period_po_qty as 
(
    select a.item_guid as item_guid 
        , a.business_unit_address_guid as business_unit_address_guid
        , a.location_guid as location_guid
        , a.lot_guid  as lot_guid
        , b.fiscal_year_period_no as fiscal_year_period_no
        , sum(a.transaction_pri_uom_qty) as transaction_qty_po
        , sum(a.base_amt) as lookup_amt_po
        ,sum(a.transaction_kg_qty) as transaction_kg_qty_po
    from fct_wbx_inv_trans_ledger a
    , dim_date b
    where a.transaction_date = b.calendar_date
    and a.source_document_type in ('3.0.1','3.0.2','3.0.3','3.1.0','3.2.0')
    and (a.transaction_qty <> 0 or a.transaction_amt <> 0)
    group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid, b.fiscal_year_period_no
    having sum(a.transaction_qty)<>0 or sum(a.transaction_amt)<>0
),

 lkp_inv_trans_for_period_transfer_in_intra as
 (
    select 
        a.item_guid as item_guid
        , a.business_unit_address_guid as business_unit_address_guid
        , a.location_guid as location_guid
        , a.lot_guid as lot_guid
        , b.fiscal_year_period_no as fiscal_year_period_no
        , sum(a.transaction_pri_uom_qty) as transaction_qty_in
        , sum(a.base_amt) as lookup_amt_in
    from fct_wbx_inv_trans_ledger a
    , dim_date b
    where a.transaction_date = b.calendar_date
        and (a.transaction_qty <> 0 or a.transaction_amt <> 0)
        and a.document_type = 'INVTRX'
        and a.transaction_qty > 0
    group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid , b.fiscal_year_period_no
    having sum(a.transaction_qty)<>0 or sum(a.transaction_amt)<>0
),

lkp_inv_trans_for_period_transfer_out_intra as 
(
    select 
        a.item_guid as item_guid
        , a.business_unit_address_guid as business_unit_address_guid
        , a.location_guid as location_guid
        , a.lot_guid as lot_guid
        , b.fiscal_year_period_no as fiscal_year_period_no
        , sum(a.transaction_pri_uom_qty) as transaction_qty_out
        , sum(a.base_amt) as lookup_amt_out
    from fct_wbx_inv_trans_ledger a
    , dim_date b
    where a.transaction_date = b.calendar_date
        and (a.transaction_qty <> 0 or a.transaction_amt <> 0)
        and a.document_type = 'INVTRX'
        and a.transaction_qty < 0
    group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid, b.fiscal_year_period_no
    having sum(a.transaction_qty)<>0 or sum(a.transaction_amt)<>0
) ,

lkp_inv_trans_on_hand_beginning as
 (
    select 
        a.item_guid as item_guid 
        , a.business_unit_address_guid as business_unit_address_guid
        , a.location_guid as location_guid
        , a.lot_guid as lot_guid
        , date_range.fiscal_year_period_no as fiscal_year_period_no
        , sum(a.transaction_pri_uom_qty) as transaction_qty_beginning
        , sum(a.base_amt) as lookup_amt_beginning
        , sum(a.transaction_kg_qty) as transaction_kg_qty_beginning
    from fct_wbx_inv_trans_ledger a
    , date_range
    where a.transaction_date <= date_range.fiscal_period_end_dt
        and (a.transaction_qty <> 0 or a.transaction_amt <> 0)
        and a.document_type in ('INVRCPT','INVCOUNT','INVTRX','INVADJ')
    group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid, date_range.fiscal_year_period_no
    having sum(a.transaction_qty)<>0 or sum(a.transaction_amt)<>0
),

base_data as 
(
    select distinct a.source_item_identifier, a.item_guid
        , a.source_business_unit_code, a.business_unit_address_guid
        , case when trim(a.source_location_code)='' then '-' else a.source_location_code end as source_location_code
        , a.location_guid
        ,  case when trim(a.source_lot_code)='' then '-' else a.source_lot_code end source_lot_code
        , a.lot_guid
        , date_range.fiscal_year_period_no
        , date_range.fiscal_period_no
        , date_range.fiscal_period_begin_dt
        , date_range.fiscal_period_end_dt,
        case 
            when date_range.fiscal_period_no=1 
            then date_range.fiscal_year_period_no-89 
            else date_range.fiscal_year_period_no-1 
            end as prv_fiscal_period_number
    from fct_wbx_inv_trans_ledger a
    , date_range
    where a.transaction_date <= date_range.fiscal_period_end_dt
),

lkp_inv_trans_on_hand_ending as
 (
    select 
        a.item_guid as item_guid
        , a.business_unit_address_guid as business_unit_address_guid
        , a.location_guid as location_guid
        , a.lot_guid as lot_guid
        , date_range.fiscal_year_period_no as fiscal_year_period_no
        , sum(a.transaction_pri_uom_qty) as transaction_qty_on_hand
        , sum(a.base_amt) as lookup_amt_on_hand
        , sum(a.transaction_kg_qty) as transaction_kg_qty_on_hand
    from fct_wbx_inv_trans_ledger a
    , date_range
    where a.transaction_date <= date_range.fiscal_period_end_dt
    and (a.transaction_qty <> 0 or a.transaction_amt <> 0)
    and a.document_type in ('INVRCPT','INVCOUNT','INVTRX','INVADJ') --CNR, check on document type. Group by on new trans ledger in snowflake on doc type
    group by a.item_guid, a.business_unit_address_guid, a.location_guid, a.lot_guid, date_range.fiscal_year_period_no
    having sum(a.transaction_qty)<>0 or sum(a.transaction_amt)<>0
),

joined_source as ( 
    select  
        '{{ env_var("DBT_SOURCE_SYSTEM") }}' AS SOURCE_SYSTEM ,
        base.SOURCE_ITEM_IDENTIFIER,
        base.ITEM_GUID, 
        base.SOURCE_BUSINESS_UNIT_CODE,
        base.BUSINESS_UNIT_ADDRESS_GUID, 
        nvl(base.SOURCE_LOCATION_CODE,'-') as SOURCE_LOCATION_CODE, 
        base.LOCATION_GUID
        ,nvl(base.SOURCE_LOT_CODE,'-') as SOURCE_LOT_CODE, 
        base.LOT_GUID,
        base.FISCAL_YEAR_PERIOD_NO as FISCAL_PERIOD_NUMBER, 
        base.FISCAL_PERIOD_BEGIN_DT,
        base.FISCAL_PERIOD_END_DT,
        base.PRV_FISCAL_PERIOD_NUMBER,
        nvl(period_ledger.TRANSACTION_QTY,0) as LEDGER_QTY,  
        nvl(period_ledger.TRANSACTION_KG_QTY,0) as LEDGER_KG_QTY,
        nvl(period_ledger.LOOKUP_AMT,0) as LEDGER_AMT,
        nvl(period_po_qty.TRANSACTION_QTY_PO,0) as PO_RECEIPT_QTY,
        nvl(period_po_qty.LOOKUP_AMT_PO,0) as PO_RECEIPT_AMT,
        nvl(period_po_qty.TRANSACTION_KG_QTY_PO,0) as PO_RECEIPT_KG_QTY,
        nvl(trfr_in_intra.TRANSACTION_QTY_IN,0) as TRANSFER_IN_INTRACOMPANY_QTY,
        nvl(trfr_in_intra.LOOKUP_AMT_IN,0) as TRANSFER_IN_INTRACOMPANY_AMT,
        nvl(abs(trfr_out_intra.TRANSACTION_QTY_OUT),0) as TRANSFER_OUT_INTRACOMPANY_QTY,
        nvl(trfr_out_intra.LOOKUP_AMT_OUT,0) as TRANSFER_OUT_INTRACOMPANY_AMT,
        nvl(hand_beginning.TRANSACTION_QTY_BEGINNING,0) as BEGINNING_INVENTORY_QTY,
        nvl(hand_beginning.LOOKUP_AMT_BEGINNING,0) as BEGINNING_INVENTORY_AMT,
        nvl(hand_beginning.TRANSACTION_KG_QTY_BEGINNING,0) as BEGINNING_INVENTORY_KG_QTY,
        nvl(hand_ending.TRANSACTION_QTY_ON_HAND,0) as ENDING_INVENTORY_QTY,
        nvl(hand_ending.LOOKUP_AMT_ON_HAND,0) as ENDING_INVENTORY_AMT,
        nvl(hand_ending.TRANSACTION_KG_QTY_ON_HAND,0) as ENDING_INVENTORY_KG_QTY,
        0 as TRANSFER_IN_INTERCOMPANY_QTY	,	 
        0 as TRANSFER_IN_INTERCOMPANY_AMT   ,
        0 as TRANSFER_OUT_INTERCOMPANY_QTY	,		 
        0 as TRANSFER_OUT_INTERCOMPANY_AMT	,
        current_date as LOAD_DATE,
        current_date as UPDATE_DATE ,
        null as SOURCE_UPDATED_D_ID,
        0 as PO_ALL_RECEIPT_QTY,
        '-' as TRANSACTION_CURRENCY,
        '-' as TRANSACTION_UOM,
        case when
        nvl(period_ledger.TRANSACTION_QTY,0) = 0 and 
        nvl(period_ledger.LOOKUP_AMT,0) = 0 and 
        nvl(period_po_qty.TRANSACTION_QTY_PO,0) = 0 and 
        nvl(period_po_qty.LOOKUP_AMT_PO,0) = 0 and
        nvl(abs(trfr_out_intra.TRANSACTION_QTY_OUT),0) = 0 and  
        nvl(trfr_out_intra.LOOKUP_AMT_OUT,0) = 0 and  
        nvl(hand_beginning.TRANSACTION_QTY_BEGINNING,0) = 0 and 
        nvl(hand_beginning.LOOKUP_AMT_BEGINNING,0) = 0 and 
        nvl(hand_ending.TRANSACTION_QTY_ON_HAND,0) = 0 and 
        nvl(hand_ending.LOOKUP_AMT_ON_HAND,0) = 0  
        then 1 else 0 end as  FLAG

    from base_data base 
    left outer join lkp_inv_trans_for_period_ledger period_ledger
    on period_ledger.item_guid	=	base.item_guid	 
        and period_ledger.business_unit_address_guid	=	base.business_unit_address_guid	 
        and period_ledger.location_guid	=	base.location_guid	 
        and period_ledger.lot_guid	=	base.lot_guid	 
        and period_ledger.fiscal_year_period_no	=	base.fiscal_year_period_no 
    left outer join lkp_inv_trans_for_period_po_qty as period_po_qty
    on period_po_qty.item_guid	=	base.item_guid	 
        and period_po_qty.business_unit_address_guid	=	base.business_unit_address_guid	 
        and period_po_qty.location_guid	=	base.location_guid	 
        and period_po_qty.lot_guid	=	base.lot_guid	 
        and period_po_qty.fiscal_year_period_no	=	base.fiscal_year_period_no  
    left outer join lkp_inv_trans_for_period_transfer_in_intra trfr_in_intra
    on trfr_in_intra.item_guid	=	base.item_guid	 
        and trfr_in_intra.business_unit_address_guid	=	base.business_unit_address_guid	 
        and trfr_in_intra.location_guid	=	base.location_guid	 
        and trfr_in_intra.lot_guid	=	base.lot_guid	 
        and trfr_in_intra.fiscal_year_period_no	=	base.fiscal_year_period_no  
    left outer join lkp_inv_trans_for_period_transfer_out_intra trfr_out_intra
    on trfr_out_intra.item_guid	= base.item_guid
        and trfr_out_intra.business_unit_address_guid = base.business_unit_address_guid	 
        and trfr_out_intra.location_guid = base.location_guid	 
        and trfr_out_intra.lot_guid	= base.lot_guid	 
        and trfr_out_intra.fiscal_year_period_no =	base.fiscal_year_period_no 
    left outer join lkp_inv_trans_on_hand_beginning hand_beginning
    on hand_beginning.item_guid	= base.item_guid
        and hand_beginning.business_unit_address_guid = base.business_unit_address_guid	 
        and hand_beginning.location_guid = base.location_guid	 
        and hand_beginning.lot_guid	= base.lot_guid	 
        and hand_beginning.fiscal_year_period_no = base.prv_fiscal_period_number 
    left outer join lkp_inv_trans_on_hand_ending hand_ending
    on hand_ending.item_guid = base.item_guid
        and hand_ending.business_unit_address_guid = base.business_unit_address_guid	 
        and hand_ending.location_guid = base.location_guid	 
        and hand_ending.lot_guid = base.lot_guid	 
        and hand_ending.fiscal_year_period_no =	base.fiscal_year_period_no 
),

target as (select 

    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

    cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,

    cast(substring(source_location_code,1,255) as text(255) ) as source_location_code  ,

    cast(location_guid as text(255) ) as location_guid  ,

    cast(substring(source_lot_code,1,255) as text(255) ) as source_lot_code  ,

    cast(lot_guid as text(255) ) as lot_guid  ,

    cast(fiscal_period_number as number(38,0) ) as fiscal_period_number  ,

    cast(substring(transaction_currency,1,255) as text(255) ) as transaction_currency  ,

    cast(substring(transaction_uom,1,255) as text(255) ) as transaction_uom  ,

    cast(ledger_qty as number(38,10) ) as ledger_qty  ,

    cast(ledger_amt as number(38,10) ) as ledger_amt  ,

    cast(beginning_inventory_qty as number(38,10) ) as beginning_inventory_qty  ,

    cast(beginning_inventory_amt as number(38,10) ) as beginning_inventory_amt  ,

    cast(ending_inventory_qty as number(38,10) ) as ending_inventory_qty  ,

    cast(ending_inventory_amt as number(38,10) ) as ending_inventory_amt  ,

    cast(po_receipt_qty as number(38,10) ) as po_receipt_qty  ,

    cast(po_receipt_amt as number(38,10) ) as po_receipt_amt  ,

    cast(transfer_in_intercompany_qty as number(38,10) ) as transfer_in_intercompany_qty  ,

    cast(transfer_in_intercompany_amt as number(38,10) ) as transfer_in_intercompany_amt  ,

    cast(transfer_out_intercompany_qty as number(38,10) ) as transfer_out_intercompany_qty  ,

    cast(transfer_out_intercompany_amt as number(38,10) ) as transfer_out_intercompany_amt  ,

    cast(transfer_in_intracompany_qty as number(38,10) ) as transfer_in_intracompany_qty  ,

    cast(transfer_in_intracompany_amt as number(38,10) ) as transfer_in_intracompany_amt  ,

    cast(transfer_out_intracompany_qty as number(38,10) ) as transfer_out_intracompany_qty  ,

    cast(transfer_out_intracompany_amt as number(38,10) ) as transfer_out_intracompany_amt  ,

    cast(load_date as timestamp_ntz(9) ) as load_date  ,

    cast(update_date as timestamp_ntz(9) ) as update_date  ,

    cast(source_updated_d_id as number(38,0) ) as source_updated_d_id  ,

    cast(beginning_inventory_kg_qty as number(38,10) ) as beginning_inventory_kg_qty  ,

    cast(ending_inventory_kg_qty as number(38,10) ) as ending_inventory_kg_qty  ,

    cast(po_receipt_kg_qty as number(38,10) ) as po_receipt_kg_qty  ,

    cast(ledger_kg_qty as number(38,10) ) as ledger_kg_qty  ,

    cast(  {{ dbt_utils.surrogate_key(
            ["SOURCE_SYSTEM","SOURCE_ITEM_IDENTIFIER","SOURCE_BUSINESS_UNIT_CODE","SOURCE_LOCATION_CODE","SOURCE_LOT_CODE","FISCAL_PERIOD_NUMBER"])
            }}  as text(255) ) as unique_key
from joined_source
where flag =0) 

select * from target