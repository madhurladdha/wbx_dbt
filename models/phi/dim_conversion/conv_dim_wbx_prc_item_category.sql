{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}


with
    old_dim as (
        select  *
        from {{ source("WBX_PROD", "dim_wbx_prc_item_category") }} where {{env_var("DBT_PICK_FROM_CONV")}}='Y'
    ),

    old_plant as (
        select distinct 
        source_system, 
        source_business_unit_code,
        source_business_unit_code_new, 
        plantdc_address_guid,
        plantdc_address_guid_new 
        from {{ ref('conv_dim_wbx_plant_dc') }} where type != 'COST CENTER' 
    ),

    old_item as (
        select distinct 
        item_guid, 
        item_guid_old, 
        source_system, 
        source_item_identifier
        from {{ ref("conv_dim_wbx_item") }} 
    ),

    /*
    Creates the new item guid by using the source_item_identifier from the old item master.
    Creates the new business unit guid by using the source_business_unit_code from the old dim table.
    Adding the source_item_identifier to this set as it does not exisnt on the old dim table.
    Filters out any rows from the old item-branch combinations where the Plant value is a Cost Center as those are n/a here.
*/
    converted_dim as (
        select
            a.source_system,
            a.item_guid as item_guid_old,
            {{dbt_utils.surrogate_key(["a.source_system", "c.source_item_identifier"])}} as item_guid,
            a.business_unit_address_guid as business_unit_address_guid_old,
            b.plantdc_address_guid_new as business_unit_address_guid,
            c.source_item_identifier,
            b.source_business_unit_code,
            a.highlevel_category_code,
            a.midlevel_category_code,
            a.lowlevel_category_code,
            a.master_planning_family_code,
            a.packaging_die_size_code,
            a.safety_stock,
            a.lead_time,
            a.master_reporting_category,
            a.alternate_reporting_category,
            a.update_date,
            a.load_date,
            a.buyer_name,
            item_category_1,
            item_category_2,
            item_category_3,
            item_category_4,
            item_category_5,
            item_category_6,
            item_category_7,
            item_category_8,
            item_category_9,
            item_category_10

        from old_dim a
        join
            old_plant b
            on a.source_system = b.source_system
            and a.business_unit_address_guid = b.plantdc_address_guid  -- keep this join in so cost center is not included in conv table
        join
            old_item c
            on a.source_system = c.source_system
            and a.item_guid = c.item_guid
    )

select
    {{ dbt_utils.surrogate_key(["item_guid", "business_unit_address_guid"]) }} as unique_key, * from converted_dim 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY source_business_unit_code) = 1
