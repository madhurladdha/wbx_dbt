{{
    config(
        materialized=env_var("DBT_MAT_VIEW"),
    )
}}

with
    old_fct as (
        select * from {{ source("FACTS_FOR_COMPARE", "inv_wtx_supply_sched_dly_fact") }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
    )

select

    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(snapshot_date as date) as snapshot_date,

    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,

    cast(
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,

    cast(substring(variant_code, 1, 255) as text(255)) as variant_code,

    cast(transaction_date as date) as transaction_date,

    cast(substring(source_company_code, 1, 255) as text(255)) as source_company_code,

    cast(substring(source_site_code, 1, 255) as text(255)) as source_site_code,

    cast(substring(plan_version, 1, 255) as text(255)) as plan_version,

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
    ) as business_unit_address_guid,

    cast(transaction_quantity as number(38, 10)) as transaction_quantity,

    cast(original_quantity as number(38, 10)) as original_quantity,

    cast(onhand_qty as number(38, 10)) as onhand_qty,

    cast(demand_transit_qty as number(38, 10)) as demand_transit_qty,

    cast(supply_transit_qty as number(38, 10)) as supply_transit_qty,

    cast(expired_qty as number(38, 10)) as expired_qty,

    cast(blocked_qty as number(38, 10)) as blocked_qty,

    cast(demand_wo_qty as number(38, 10)) as demand_wo_qty,

    cast(production_wo_qty as number(38, 10)) as production_wo_qty,

    cast(production_planned_wo_qty as number(38, 10)) as production_planned_wo_qty,

    cast(demand_planned_batch_wo_qty as number(38, 10)) as demand_planned_batch_wo_qty,

    cast(prod_planned_batch_wo_qty as number(38, 10)) as prod_planned_batch_wo_qty,

    cast(supply_trans_journal_qty as number(38, 10)) as supply_trans_journal_qty,

    cast(demand_trans_journal_qty as number(38, 10)) as demand_trans_journal_qty,

    cast(demand_planned_trans_qty as number(38, 10)) as demand_planned_trans_qty,

    cast(supply_stock_journal_qty as number(38, 10)) as supply_stock_journal_qty,

    cast(demand_stock_journal_qty as number(38, 10)) as demand_stock_journal_qty,

    cast(supply_planned_po_qty as number(38, 10)) as supply_planned_po_qty,

    cast(supply_po_qty as number(38, 10)) as supply_po_qty,

    cast(supply_po_transfer_qty as number(38, 10)) as supply_po_transfer_qty,

    cast(supply_po_return_qty as number(38, 10)) as supply_po_return_qty,

    cast(sales_order_qty as number(38, 10)) as sales_order_qty,

    cast(return_sales_order_qty as number(38, 10)) as return_sales_order_qty,

    cast(minimum_stock_qty as number(38, 10)) as minimum_stock_qty,

    cast(load_date as date) as load_date,

    cast(update_date as date) as update_date,

    cast(
        demand_unplanned_batch_wo_qty as number(38, 10)
    ) as demand_unplanned_batch_wo_qty,

    cast(
        {{
            dbt_utils.surrogate_key(
                [
                    "source_system",
                    "snapshot_date",
                    "source_business_unit_code",
                    "source_item_identifier",
                    "variant_code",
                    "transaction_date",
                    "source_company_code",
                    "source_site_code",
                    "plan_version",
                ]
            )
        }} as text(255)
    ) as unique_key

from old_fct
