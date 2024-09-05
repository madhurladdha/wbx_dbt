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



with int_fact as (
    select *
    from {{ ref('int_f_wbx_inv_aging') }} qualify
        row_number() over (partition by unique_key order by 1) = 1
),

/* Pulls the corresponding AX history across to blend w/ the D365 transactional data.
    For the conv model, the relevant dimension values have been converted, where that is applicable.
*/
old_ax_fact as (
    select * from {{ ref('conv_fct_wbx_inv_aging') }}
),

int as (
    select
        source_system,
        inventory_snapshot_date,
        {{ dbt_utils.surrogate_key(["source_system","source_item_identifier"]) }}  as item_guid,
        source_item_identifier,
       {{ dbt_utils.surrogate_key(["source_system","SOURCE_BUSINESS_UNIT_CODE","'PLANT_DC'"]) }} as  business_unit_address_guid,
        source_business_unit_code, --will need a conversion model 
        source_lot_code,
         {{ dbt_utils.surrogate_key(["SOURCE_SYSTEM","SOURCE_BUSINESS_UNIT_CODE","SOURCE_ITEM_IDENTIFIER","SOURCE_LOT_CODE"]) }}  as lot_guid,
        transaction_uom,
        on_hand_qty,
        transaction_date,
        lot_expiration_date,
        shelf_life_days,
        manufactured_date,
        prod_age_in_weeks,
        prod_age_in_days,
        ship_to_guideline,
        salvage_date,
        weeks_left,
        inventory_age_status,
        load_date,
        update_date,
        source_updated_d_id,
        on_hand_kg_qty,
        unique_key,
        'D365' as source_legacy
        from int_fact
),

ax_hist as (
    select
        a.source_system,
        a.inventory_snapshot_date,
        a.item_guid,
        a.source_item_identifier,
        a.business_unit_address_guid,
        a.source_business_unit_code,
        a.source_lot_code,
        a.lot_guid,
        a.transaction_uom,
        a.on_hand_qty,
        a.transaction_date,
        a.lot_expiration_date,
        a.shelf_life_days,
        a.manufactured_date,
        a.prod_age_in_weeks,
        a.prod_age_in_days,
        a.ship_to_guideline,
        a.salvage_date,
        a.weeks_left,
        a.inventory_age_status,
        a.load_date,
        a.update_date,
        a.source_updated_d_id,
        a.on_hand_kg_qty,
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
        cast(substring(source_system,1,255) as text(255) ) as source_system  ,
        cast(inventory_snapshot_date as timestamp_ntz(9) ) as inventory_snapshot_date  ,
        cast(substring(item_guid,1,255) as text(255) ) as item_guid  ,
        cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
        cast(substring(business_unit_address_guid,1,255) as text(255) ) as business_unit_address_guid  ,
        cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
        cast(substring(source_lot_code,1,255) as text(255) ) as source_lot_code  ,
        cast(substring(lot_guid,1,255) as text(255) ) as lot_guid  ,
        cast(substring(transaction_uom,1,20) as text(20) ) as transaction_uom  ,
        cast(on_hand_qty as number(27,2) ) as on_hand_qty  ,
        cast(transaction_date as timestamp_ntz(9) ) as transaction_date  ,
        cast(lot_expiration_date as timestamp_ntz(9) ) as lot_expiration_date  ,
        cast(shelf_life_days as number(38,10) ) as shelf_life_days  ,
        cast(manufactured_date as timestamp_ntz(9) ) as manufactured_date  ,
        cast(prod_age_in_weeks as number(38,0) ) as prod_age_in_weeks  ,
        cast(prod_age_in_days as number(38,0) ) as prod_age_in_days  ,
        cast(ship_to_guideline as number(38,10) ) as ship_to_guideline  ,
        cast(salvage_date as timestamp_ntz(9) ) as salvage_date  ,
        cast(weeks_left as number(38,0) ) as weeks_left  ,
        cast(substring(inventory_age_status,1,20) as text(20) ) as inventory_age_status  ,
        cast(load_date as timestamp_ntz(9) ) as load_date  ,
        cast(update_date as timestamp_ntz(9) ) as update_date  ,
        cast(source_updated_d_id as number(38,0) ) as source_updated_d_id  ,
        cast(on_hand_kg_qty as number(27,2) ) as on_hand_kg_qty,
        cast(substring(unique_key,1,255) as text(255) ) as unique_key,
        cast(substring(source_legacy, 1, 255) as text(255)) as source_legacy
    from final