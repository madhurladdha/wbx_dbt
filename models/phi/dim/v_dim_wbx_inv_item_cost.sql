{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    tags=["inventory", "item_cost","inv_item_cost"]
    )
}}

/*
    This is a view on top of the Weetabix Inventory Item Cost dimension model, dim_wbx_inv_item_cost, just to alias the field names for 
    effective_date and expiration_date so that it aligns with field names and logic for anything downstream.  Ideally, downstream models
    would start to simply use the model itself, but this view facilitates easy adjustment if needed.
*/

with scd2 as 
(
    select * from {{ref('dim_wbx_inv_item_cost')}}
),

prep as
(
    select
        unique_key,
        source_system,
        source_item_identifier,
        item_guid,
        source_location_code,
        location_guid,
        source_lot_code,
        lot_guid,
        source_business_unit_code,
        business_unit_address_guid,
        source_cost_method_code,
        target_cost_method_code,
        target_cost_method_desc,
        item_unit_cost,
        phi_currency,
        oc_base_conv_rt,
        transaction_currency,
        variant_code,
        base_currency,
        pcomp_currency,
        oc_trans_conv_rt,
        oc_corp_conv_rt,
        oc_pcomp_conv_rt,
        oc_base_item_unit_prim_cost,
        oc_corp_item_unit_prim_cost,
        oc_pcomp_item_unit_prim_cost,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from as eff_date,
        nvl(dbt_valid_to,to_date('31-DEC-2050')) as expir_date
    from scd2
)

select * from prep
