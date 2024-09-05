

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'inv_wtx_pct_wkly_agg') }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'

),

renamed as (

    select
        source_system,
        snapshot_date,
        source_item_identifier,
        variant_code,
        source_company_code,
        plan_version,
        {{ dbt_utils.surrogate_key(["source_system","source_item_identifier"]) }}  as item_guid, 
        week_description,
        week_start_dt,
        week_end_dt,
        week_start_stock,
        demand_transit_qty,
        supply_transit_qty,
        expired_qty,
        blocked_qty,
        pa_eff_week_qty,
        pa_balance_qty,
        virtual_stock_qty,
        demand_wo_qty,
        production_wo_qty,
        production_planned_wo_qty,
        demand_planned_batch_wo_qty,
        demand_unplanned_batch_wo_qty,
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
        current_week_flag,
        load_date,
        update_date,
        period_end_firm_purchase_qty,
        base_currency,
        phi_currency,
        pcomp_currency,
        oc_base_item_unit_prim_cost,
        oc_corp_item_unit_prim_cost,
        oc_pcomp_item_unit_prim_cost,
        agreement_flag,
        supply_stock_adj_qty,
        demand_stock_adj_qty
    from source

)

select * from renamed

