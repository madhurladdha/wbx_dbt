{{
    config(
        materialized = env_var('DBT_MAT_VIEW')
    )
}}

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'inv_wtx_item_cost_dim') }} where {{env_var("DBT_PICK_FROM_CONV")}}='Y'

),

renamed as (

    select
        source_system,
        source_item_identifier,
        source_business_unit_code,
        source_location_code,
        source_lot_code,
        {{ dbt_utils.surrogate_key(['source_system','source_item_identifier']) }} as item_guid,
        {{ dbt_utils.surrogate_key(['source_system','source_location_code','source_business_unit_code']) }} as location_guid,
        {{ dbt_utils.surrogate_key(['source_system','source_business_unit_code','source_item_identifier','source_lot_code']) }} as lot_guid,
        {{ dbt_utils.surrogate_key(['source_system','source_business_unit_code',"'PLANT_DC'"]) }} AS BUSINESS_UNIT_ADDRESS_GUID,
        source_cost_method_code,
        target_cost_method_code,
        target_cost_method_desc,
        item_unit_cost,
        eff_date,
        eff_d_id,
        expir_date,
        expir_d_id,
        source_updated_date,
        source_updated_d_id,
        transaction_currency,
        base_currency,
        phi_currency,
        pcomp_currency,
        oc_trans_conv_rt,
        oc_base_conv_rt,
        oc_corp_conv_rt,
        oc_pcomp_conv_rt,
        oc_base_item_unit_prim_cost,
        oc_corp_item_unit_prim_cost,
        oc_pcomp_item_unit_prim_cost,
        load_date,
        update_date,
        variant_code

    from source

)

select  {{ dbt_utils.surrogate_key(['source_item_identifier','source_business_unit_code','source_location_code','source_lot_code','source_cost_method_code','variant_code']) }} as unique_key,*
from  renamed
