{{ config(    
    tags=["inventory", "trans_ledger","inv_daily_balance","inv_aging"] ,
    snowflake_warehouse= env_var("DBT_WBX_SF_WH")
 ) }}

with daily_balance as 
(
    select * from {{ ref("fct_wbx_inv_daily_balance") }}
),

dim_wbx_item as (select * from {{ ref("dim_wbx_item") }}),
dim_wbx_lot as (select * from {{ ref("dim_wbx_lot") }}),
curr_dim_date as (select * from {{ ref("src_dim_date") }} where calendar_date = current_date),

daily_balance_aggr as 
(
    select
        source_system as source_system,
        inventory_snapshot_date as inventory_snapshot_date,
        item_guid as item_guid,
        source_item_identifier as source_item_identifier,
        business_unit_address_guid as business_unit_address_guid,
        source_business_unit_code as source_business_unit_code,
        source_lot_code as source_lot_code,
        lot_guid as lot_guid,
        primary_uom as primary_uom,
        sum(on_hand_qty) as on_hand_qty,
        transaction_date as transaction_date,
        sum(on_hand_kg_qty) as on_hand_kg_qty,update_date
    from daily_balance
    where inventory_snapshot_date = to_date(convert_timezone('UTC',current_timestamp))
    group by source_system,inventory_snapshot_date,item_guid,source_item_identifier,business_unit_address_guid,source_business_unit_code,
    source_lot_code,lot_guid,primary_uom,transaction_date,update_date 
),

joined_source as 
 (
    select
    trans.source_system,
    trans.inventory_snapshot_date,
    trans.item_guid,
    trans.source_item_identifier,
    trans.business_unit_address_guid,
    trans.source_business_unit_code,
    trans.source_lot_code,
    trans.lot_guid,
    trans.primary_uom as transaction_uom,
    trans.on_hand_qty,
    trans.transaction_date,
    trans.on_hand_kg_qty,
    dim_wbx_lot.lot_expiration_date as lot_expiration_date,
    dim_wbx_item.shelf_life_days as shelf_life_days,
    dateadd(day,-shelf_life_days,dim_wbx_lot.lot_expiration_date) as manufactured_date,
    datediff(week,manufactured_date,trans.inventory_snapshot_date) as  prod_age_in_weeks,
    datediff(day,manufactured_date,trans.inventory_snapshot_date)  as prod_age_in_days,
    dim_wbx_item.shelf_life_days as ship_to_guideline,
    dim_wbx_lot.lot_expiration_date  as salvage_date,
    trunc(dim_wbx_item.shelf_life_days/7,0) - prod_age_in_weeks as weeks_left,
    current_date as load_date,
    current_date as update_date,
    case when weeks_left >=12 then  'OK'
    when weeks_left   >= 8 and weeks_left <12 then 'Within 12 Weeks'
    when weeks_left  >=4  and weeks_left <8 then  'Within 8 Weeks'
    when weeks_left  >= 1  and weeks_left <4 then 'Within 4 Weeks'
    else 'Aged' end as inventory_age_status,
    calendar_date_id as source_updated_d_id,
    {{dbt_utils.surrogate_key(
        [
            "trans.source_system",
            "trans.inventory_snapshot_date",
            "trans.source_item_identifier",
            "trans.source_business_unit_code",
            "trans.source_lot_code"
        ]
    )
    }} as unique_key

    from daily_balance_aggr trans
    left outer join dim_wbx_lot dim_wbx_lot
        on dim_wbx_lot.lot_guid  = trans.lot_guid
        and dim_wbx_lot.item_guid   = trans.item_guid
        and dim_wbx_lot.business_unit_address_guid  = trans.business_unit_address_guid
    left outer join dim_wbx_item dim_wbx_item
        on dim_wbx_item.source_system	=	trans.source_system	 
        and dim_wbx_item.item_guid	=	trans.item_guid	 
        and dim_wbx_item.business_unit_address_guid	=	trans.business_unit_address_guid
    left outer join curr_dim_date
        on 1=1
 )

select * from joined_source

