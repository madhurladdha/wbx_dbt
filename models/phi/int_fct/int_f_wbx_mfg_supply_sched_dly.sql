{{ config(tags=["manufacturing", "supply_Schedule", "wbx", "daily","inventory"]) }}

with
    source as (
        select
            source_system,
            snapshot_date,
            source_business_unit_code,
            source_item_identifier,
            variant_code,
            transaction_date,
            source_company_code,
            source_site_code,
            plan_version,
            sum(transaction_quantity) as transaction_quantity,
            sum(original_quantity) as original_quantity,
            sum(onhand_qty) as onhand_qty,
            sum(demand_transit_qty) * -1 as demand_transit_qty,
            sum(supply_transit_qty) as supply_transit_qty,
            sum(expired_qty) * -1 as expired_qty,
            sum(demand_wo_qty) * -1 as demand_wo_qty,
            sum(production_wo_qty) as production_wo_qty,
            sum(production_planned_wo_qty) as production_planned_wo_qty,
            sum(demand_planned_batch_wo_qty) * -1 as demand_planned_batch_wo_qty,
            sum(demand_unplanned_batch_wo_qty) * -1 as demand_unplanned_batch_wo_qty,
            sum(prod_planned_batch_wo_qty) as prod_planned_batch_wo_qty,
            sum(supply_trans_journal_qty) as supply_trans_journal_qty,
            sum(demand_trans_journal_qty) * -1 as demand_trans_journal_qty,
            sum(demand_planned_trans_qty) * -1 as demand_planned_trans_qty,
            sum(supply_stock_journal_qty) as supply_stock_journal_qty,
            sum(demand_stock_journal_qty) * -1 as demand_stock_journal_qty,
            sum(supply_po_qty) as supply_po_qty,
            sum(supply_planned_po_qty) as supply_planned_po_qty,
            sum(supply_po_transfer_qty) as supply_po_transfer_qty,
            sum(supply_po_return_qty) * -1 as supply_po_return_qty,
            sum(sales_order_qty) * -1 as sales_order_qty,
            sum(return_sales_order_qty) as return_sales_order_qty,
            sum(minimum_stock_qty) * -1 as minimum_stock_qty
        from
            (
                select
                    source_system,
                    snapshot_date,
                    source_business_unit_code,
                    source_item_identifier,
                    variant_code,
                    source_company_code,
                    source_site_code,
                    transaction_date,
                    transaction_type_code,
                    transaction_desc,
                    plan_version,
                    transaction_direction_code,
                    transaction_direction_desc,
                    transaction_quantity,
                    original_quantity,
                    case
                        when
                            transaction_type_code = 1 and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as onhand_qty,
                    case
                        when
                            transaction_type_code = 16
                            and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as demand_transit_qty,
                    case
                        when
                            transaction_type_code = 17
                            and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as supply_transit_qty,
                    case
                        when
                            transaction_type_code = 47
                            and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as expired_qty,
                    case
                        when
                            transaction_type_code = 12
                            and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as demand_wo_qty,
                    case
                        when
                            transaction_type_code = 9 and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as production_wo_qty,
                    case
                        when
                            transaction_type_code = 31
                            and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as production_planned_wo_qty,
                    case
                        when
                            transaction_type_code = 45
                            and transaction_direction_code = 2
                            and transaction_planned_flag = 'PLANNED'
                        then transaction_quantity
                        else 0
                    end as demand_planned_batch_wo_qty,
                    case
                        when
                            transaction_type_code = 45
                            and transaction_direction_code = 2
                            and transaction_planned_flag = 'UNPLANNED'
                        then transaction_quantity
                        else 0
                    end as demand_unplanned_batch_wo_qty,
                    case
                        when
                            transaction_type_code = 46
                            and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as prod_planned_batch_wo_qty,
                    case
                        when
                            transaction_type_code = 15
                            and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as supply_trans_journal_qty,
                    case
                        when
                            transaction_type_code = 15
                            and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as demand_trans_journal_qty,
                    case
                        when
                            transaction_type_code = 35
                            and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as demand_planned_trans_qty,
                    case
                        when
                            transaction_type_code = 13
                            and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as supply_stock_journal_qty,
                    case
                        when
                            transaction_type_code = 13
                            and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as demand_stock_journal_qty,
                    case
                        when
                            transaction_type_code = 8 and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as supply_po_qty,
                    case
                        when
                            transaction_type_code = 33
                            and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as supply_planned_po_qty,
                    case
                        when
                            transaction_type_code = 34
                            and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as supply_po_transfer_qty,
                    case
                        when
                            transaction_type_code = 8 and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as supply_po_return_qty,
                    case
                        when
                            transaction_type_code = 10
                            and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as sales_order_qty,
                    case
                        when
                            transaction_type_code = 10
                            and transaction_direction_code = 1
                        then transaction_quantity
                        else 0
                    end as return_sales_order_qty,
                    case
                        when
                            transaction_type_code = 14
                            and transaction_direction_code = 2
                        then transaction_quantity
                        else 0
                    end as minimum_stock_qty
                from {{ ref("stg_f_wbx_mfg_supply_sched_dly") }}
            )
        group by
            source_system,
            snapshot_date,
            source_business_unit_code,
            source_item_identifier,
            variant_code,
            transaction_date,
            source_company_code,
            source_site_code,
            plan_version
    ),

    guid_generate as (
        select
            *,
            0 as blocked_qty,
            {{ dbt_utils.surrogate_key(["source_system", "source_item_identifier"]) }}
            as item_guid,
            {{
                dbt_utils.surrogate_key(
                    ["source_system", "source_business_unit_code", "'PLANT_DC'"]
                )
            }} as business_unit_address_guid,
            systimestamp() as load_date,
            systimestamp() as update_date
        from source
    ),
--casting
final as (select

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

    cast(item_guid as text(255)) as item_guid,

    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,

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
    ) as demand_unplanned_batch_wo_qty

from guid_generate)
--unique_key+final
select *,cast({{
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
            }} as varchar(255)) as unique_key from final
