-- adding manual test to check for latest snapshot only due to large volume and to add
-- filters and apply rounding offs
{{ config(enabled=false, severity="warn") }}

with
    a as (

        select
            "SOURCE_SYSTEM",
            "SNAPSHOT_DATE",
            "SOURCE_BUSINESS_UNIT_CODE",
            "SOURCE_ITEM_IDENTIFIER",
            "VARIANT_CODE",
            "SOURCE_COMPANY_CODE",
            "SOURCE_SITE_CODE",
            "PLAN_VERSION",
            "WEEK_DESC",
            "WEEK_START_DT",
            "WEEK_END_DT",
            "CURRENT_WEEK_FLAG",
            round("WEEK_START_STOCK", 4),
            round("DEMAND_TRANSIT_QTY", 4),
            round("SUPPLY_TRANSIT_QTY", 4),
            round("EXPIRED_QTY", 4),
            round("BLOCKED_QTY", 4),
            round("DEMAND_WO_QTY", 4),
            round("PRODUCTION_WO_QTY", 4),
            round("PRODUCTION_PLANNED_WO_QTY", 4),
            round("DEMAND_PLANNED_BATCH_WO_QTY", 4),
            round("PROD_PLANNED_BATCH_WO_QTY", 4),
            round("SUPPLY_TRANS_JOURNAL_QTY", 4),
            round("DEMAND_TRANS_JOURNAL_QTY", 4),
            round("DEMAND_PLANNED_TRANS_QTY", 4),
            round("SUPPLY_STOCK_JOURNAL_QTY", 4),
            round("DEMAND_STOCK_JOURNAL_QTY", 4),
            round("SUPPLY_PO_QTY", 4),
            round("SUPPLY_PLANNED_PO_QTY", 4),
            round("SUPPLY_PO_TRANSFER_QTY", 4),
            round("SUPPLY_PO_RETURN_QTY", 4),
            round("SALES_ORDER_QTY", 4),
            round("RETURN_SALES_ORDER_QTY", 4),
            round("MINIMUM_STOCK_QTY", 4),
            round("WEEK_END_STOCK", 4),
            round("PERIOD_END_FIRM_PURCHASE_QTY", 4),
            "BASE_CURRENCY",
            "PHI_CURRENCY",
            "PCOMP_CURRENCY",
            "OC_BASE_ITEM_UNIT_PRIM_COST",
            "OC_CORP_ITEM_UNIT_PRIM_COST",
            "OC_PCOMP_ITEM_UNIT_PRIM_COST",
            round("DEMAND_UNPLANNED_BATCH_WO_QTY", 4)
        from {{ ref("conv_inv_wtx_supply_sched_wkly_agg") }}
        where snapshot_date = trunc(current_date, 'dd')

    ),

    b as (

        select

            "SOURCE_SYSTEM",
            "SNAPSHOT_DATE",
            "SOURCE_BUSINESS_UNIT_CODE",
            "SOURCE_ITEM_IDENTIFIER",
            "VARIANT_CODE",
            "SOURCE_COMPANY_CODE",
            "SOURCE_SITE_CODE",
            "PLAN_VERSION",
            "WEEK_DESC",
            "WEEK_START_DT",
            "WEEK_END_DT",
            "CURRENT_WEEK_FLAG",
            round("WEEK_START_STOCK", 4),
            round("DEMAND_TRANSIT_QTY", 4),
            round("SUPPLY_TRANSIT_QTY", 4),
            round("EXPIRED_QTY", 4),
            round("BLOCKED_QTY", 4),
            round("DEMAND_WO_QTY", 4),
            round("PRODUCTION_WO_QTY", 4),
            round("PRODUCTION_PLANNED_WO_QTY", 4),
            round("DEMAND_PLANNED_BATCH_WO_QTY", 4),
            round("PROD_PLANNED_BATCH_WO_QTY", 4),
            round("SUPPLY_TRANS_JOURNAL_QTY", 4),
            round("DEMAND_TRANS_JOURNAL_QTY", 4),
            round("DEMAND_PLANNED_TRANS_QTY", 4),
            round("SUPPLY_STOCK_JOURNAL_QTY", 4),
            round("DEMAND_STOCK_JOURNAL_QTY", 4),
            round("SUPPLY_PO_QTY", 4),
            round("SUPPLY_PLANNED_PO_QTY", 4),
            round("SUPPLY_PO_TRANSFER_QTY", 4),
            round("SUPPLY_PO_RETURN_QTY", 4),
            round("SALES_ORDER_QTY", 4),
            round("RETURN_SALES_ORDER_QTY", 4),
            round("MINIMUM_STOCK_QTY", 4),
            round("WEEK_END_STOCK", 4),
            round("PERIOD_END_FIRM_PURCHASE_QTY", 4),
            "BASE_CURRENCY",
            "PHI_CURRENCY",
            "PCOMP_CURRENCY",
            "OC_BASE_ITEM_UNIT_PRIM_COST",
            "OC_CORP_ITEM_UNIT_PRIM_COST",
            "OC_PCOMP_ITEM_UNIT_PRIM_COST",
            round("DEMAND_UNPLANNED_BATCH_WO_QTY", 4)

        from {{ ref("fct_wbx_mfg_supply_sched_wkly_agg") }}
        where snapshot_date = trunc(current_date, 'dd')

    ),

    a_intersect_b as (

        select *
        from a

        intersect

        select *
        from b

    ),

    a_except_b as (

        select *
        from a

        except

        select *
        from b

    ),

    b_except_a as (

        select *
        from b

        except

        select *
        from a

    ),

    all_records as (

        select *, true as in_a, true as in_b
        from a_intersect_b

        union all

        select *, true as in_a, false as in_b
        from a_except_b

        union all

        select *, false as in_a, true as in_b
        from b_except_a

    ),

    final as (

        select *
        from all_records
        where not (in_a and in_b)
        order by in_a desc, in_b desc

    )

select *
from final
order by 1, 2, 3, 4, 5, 6, 7, 8, 9
