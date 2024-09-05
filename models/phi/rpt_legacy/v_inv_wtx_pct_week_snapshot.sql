{{ config(materialized=env_var("DBT_MAT_VIEW"), tags=["inventory", "wbx", "manufacturing", "percent", "weekly", "agg"]) }}

with
    cte_inv_wtx_pct_wkly_agg as (select * from {{ ref("fct_wbx_mfg_pct_weekly_agg") }}),  -- INV_WTX_PCT_WKLY_AGG
    cte_item as (select * from {{ ref("dim_wbx_item") }}),
    cte_supplier as (select * from {{ ref("dim_wbx_supplier") }}),
    cte_time_variant_dim as (select * from {{ ref("dim_wbx_mfg_item_variant") }}),
    cte_final as (
        select

            a.source_system,

            snapshot_date,

            a.source_item_identifier,

            d.item_type,

            c.item_allocation_key,

            a.variant_code,

            source_company_code,

            plan_version,

            a.item_guid,

            week_description,

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

            (

                case

                    when (c.variant_desc is null or c.variant_desc = ' ')
                    then to_char(d.description)

                    else to_char(c.variant_desc)

                end

            ) description,

            d.buyer_code,

            d.vendor_address_guid,

            e.supplier_name,

            period_end_firm_purchase_qty,

            a.demand_unplanned_batch_wo_qty,

            pa_balance_qty,

            pa_eff_week_qty,

            virtual_stock_qty,

            demand_stock_adj_qty,

            supply_stock_adj_qty,

            (

                supply_transit_qty
                + supply_trans_journal_qty
                + supply_stock_journal_qty
                + supply_po_qty
                + supply_planned_po_qty
                + supply_po_transfer_qty
                + return_sales_order_qty

            ) total_supply_qty,

            (

                demand_transit_qty
                + demand_wo_qty
                + demand_planned_batch_wo_qty
                + demand_trans_journal_qty
                + demand_planned_trans_qty
                + demand_stock_journal_qty
                + supply_po_return_qty

            ) total_demand_qty,

            primary_uom,

            base_currency,

            oc_base_item_unit_prim_cost,

            phi_currency,

            oc_corp_item_unit_prim_cost

        from cte_inv_wtx_pct_wkly_agg a

        inner join
            (

                select distinct
                    source_item_identifier,

                    source_system,

                    max(primary_uom) primary_uom,

                    max(item_type) item_type,

                    max(description) description,

                    max(buyer_code) buyer_code,

                    max(TRUNC(vendor_address_guid)) vendor_address_guid

                from cte_item

                where source_system = '{{env_var('DBT_SOURCE_SYSTEM')}}'

                group by source_item_identifier, source_system  /* ,DESCRIPTION*/

            ) d
            on

            a.source_item_identifier = d.source_item_identifier

            and a.source_system = d.source_system

        left join
            cte_supplier e on

            e.source_system = '{{env_var('DBT_SOURCE_SYSTEM')}}'

            and cast(ltrim(e.source_system_address_number, '0') as varchar2(255))
            = cast(d.vendor_address_guid as varchar2(255))
            and UPPER(TRIM(e.company_code)) = UPPER(TRIM(a.source_company_code))

        left join
            (

                select

                    s.source_system,

                    s.source_item_identifier,

                    trim(s.variant_code) as variant_code,

                    s.active_flag,

                    max(s.variant_desc) variant_desc,

                    max(s.item_allocation_key) item_allocation_key

                from

                    cte_time_variant_dim s,

                    (

                        select

                            source_system,

                            source_item_identifier,

                            trim(variant_code) as variant_code,

                            max(active_flag) active_flag

                        from cte_time_variant_dim

                        group by

                            source_system, source_item_identifier, trim(variant_code)

                    ) d

                where

                    s.source_item_identifier = d.source_item_identifier

                    and trim(s.variant_code) = trim(d.variant_code)

                    and s.source_system = d.source_system

                    and s.active_flag = d.active_flag

                group by

                    s.source_system,

                    s.source_item_identifier,

                    trim(s.variant_code),

                    s.active_flag

            ) c
            on

            a.source_system = c.source_system

            and a.source_item_identifier = c.source_item_identifier

            and trim(a.variant_code) = trim(c.variant_code)
    )
select
    source_system,
    snapshot_date,
    source_item_identifier,
    item_type,
    item_allocation_key,
    variant_code,
    source_company_code,
    plan_version,
    item_guid,
    week_description,
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
    description,
    buyer_code,
    vendor_address_guid,
    supplier_name,
    period_end_firm_purchase_qty,
    demand_unplanned_batch_wo_qty,
    pa_balance_qty,
    pa_eff_week_qty,
    virtual_stock_qty,
    demand_stock_adj_qty,
    supply_stock_adj_qty,
    total_supply_qty,
    total_demand_qty,
    primary_uom,
    base_currency,
    oc_base_item_unit_prim_cost,
    phi_currency,
    oc_corp_item_unit_prim_cost
from cte_final
