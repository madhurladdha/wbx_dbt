

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'inv_wtx_supply_sched_wkly_agg') }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'

),

renamed as (

    select
        source_system,
        snapshot_date,
        source_business_unit_code,
        source_item_identifier,
        variant_code,
        source_company_code,
        source_site_code,
        plan_version,
        cast(
        {{ dbt_utils.surrogate_key(["source_system", "source_item_identifier"]) }}
        as text(255)
    ) as item_guid,
        cast(
        {{
            dbt_utils.surrogate_key(
                ["source_system", "source_business_unit_code", "'PLANT_DC'"]
            )
        }} as text(255)
    ) as  business_unit_address_guid,
     cast(
        {{ dbt_utils.surrogate_key(["source_system", "source_item_identifier"]) }}
        as text(255)
    ) as v_item_guid,
        cast(
        {{
            dbt_utils.surrogate_key(
                ["source_system", "source_business_unit_code", "'PLANT_DC'"]
            )
        }} as text(255)
    ) as  v_business_unit_address_guid,
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
        period_end_firm_purchase_qty,
        base_currency,
        phi_currency,
        pcomp_currency,
        oc_base_item_unit_prim_cost,
        oc_corp_item_unit_prim_cost,
        oc_pcomp_item_unit_prim_cost,
        demand_unplanned_batch_wo_qty,
        {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "SNAPSHOT_DATE",
                        "SOURCE_BUSINESS_UNIT_CODE",
                        "SOURCE_ITEM_IDENTIFIER",
                        "VARIANT_CODE",
                        "SOURCE_COMPANY_CODE",
                        "SOURCE_SITE_CODE",
                        "PLAN_VERSION",
                        "v_ITEM_GUID",
                        "v_BUSINESS_UNIT_ADDRESS_GUID"
                    ]
                )
            }} as rw_id

    from source

)

select * from renamed

