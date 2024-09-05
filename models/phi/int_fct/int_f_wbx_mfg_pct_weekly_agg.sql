{{
    config(
        tags=["wbx", "manufacturing", "percent", "weekly", "agg"],
        materialized=env_var("DBT_MAT_TABLE"),
        transient=true
    )
}}

with
    cte_calc as (
        select
            *,
            lag(rw_id) ignore nulls over (
                order by rw_id, week_start_dt, week_end_dt, week_description
            ) as prev_rw_id,
            (
                supply_transit_qty
                + supply_trans_journal_qty
                + supply_stock_journal_qty
                + supply_planned_po_qty
                + supply_po_qty
                + supply_po_transfer_qty
                + return_sales_order_qty
            ) - (
                demand_transit_qty
                + demand_wo_qty
                + demand_planned_batch_wo_qty
                + demand_stock_journal_qty
                + demand_trans_journal_qty
                + demand_planned_trans_qty
                + supply_po_return_qty
            )
            - expired_qty
            - blocked_qty as var_week_end_stock,

            row_number() over (
                partition by rw_id
                order by rw_id, week_start_dt, week_end_dt, week_description
            ) as rank,

            pa_eff_week_qty
            - (supply_planned_po_qty + supply_po_qty) as var_pa_balance_qty,
            (supply_planned_po_qty + supply_po_qty)
            - (demand_planned_batch_wo_qty + demand_wo_qty)
            - demand_stock_adj_qty
            + supply_stock_adj_qty
            - expired_qty
            - blocked_qty as var_virtual_stock_qty

        from {{ ref("int_f_wbx_mfg_pct_weekly_pre_agg") }}
        order by rw_id, week_start_dt, week_end_dt, week_description
    ),

    int_f_wbx_mfg_pct_weekly_pre_agg as (
        select
            *,
            case
                when rank = 1 then week_end_stock else var_week_end_stock
            end as v_week_end_stock,
            nvl(
                sum(v_week_end_stock) over (
                    partition by rw_id
                    order by rw_id, week_start_dt, week_end_dt, week_description
                    rows between unbounded preceding and current row
                ),
                0
            ) as cumulative_week_end_stock,
            case
                when rank = 1 then pa_balance_qty else var_pa_balance_qty
            end as v_pa_balance_qty,
            nvl(
                sum(v_pa_balance_qty) over (
                    partition by rw_id
                    order by rw_id, week_start_dt, week_end_dt, week_description
                    rows between unbounded preceding and current row
                ),
                0
            ) as cumulative_pa_balance_qty,

            case
                when rank = 1
                then nvl(virtual_stock_qty, 0)
                else nvl(var_virtual_stock_qty, 0)
            end as v_virtual_stock_qty,
            nvl(
                sum(v_virtual_stock_qty) over (
                    partition by rw_id
                    order by rw_id, week_start_dt, week_end_dt, week_description
                    rows between unbounded preceding and current row
                ),
                0
            ) as cumulative_virtual_stock_qty
        from cte_calc
    ),

    pre_final as (
        select
            rw_id,
            prev_rw_id,
            case
                when rw_id != nvl(prev_rw_id, '')
                then week_start_stock
                else
                    lag(cumulative_week_end_stock) ignore nulls over (
                        order by rw_id, week_start_dt, week_end_dt, week_description
                    )
            end as week_start_stock_final,

            case
                when rw_id != prev_rw_id
                then week_end_stock
                else cumulative_week_end_stock
            end as week_end_stock_final,

            v_pa_balance_qty,
            case
                when rw_id != nvl(prev_rw_id, '')
                then pa_balance_qty
                when rw_id = nvl(prev_rw_id, '') and agreement_flag = 'Y'
                then cumulative_pa_balance_qty  -- lag(cumulative_PA_BALANCE_QTY) ignore nulls over ( order by rw_id, week_start_dt, week_end_dt, week_description )
                else 0
            end as pa_balance_qty_final,
            lag(pa_balance_qty_final) ignore nulls over (
                partition by rw_id
                order by rw_id, week_start_dt, week_end_dt, week_description
            ) as prev_pa_balance_qty,
            case
                when rw_id != nvl(prev_rw_id, '')
                then 0
                else pa_balance_qty_final - prev_pa_balance_qty
            end as prev_curr_diff_pa_balance_qty,
            cumulative_virtual_stock_qty,

            source_system,
            snapshot_date,
            source_item_identifier,
            variant_code,
            source_company_code,
            plan_version,
            item_guid,
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
            nvl(supply_stock_adj_qty, 0) as supply_stock_adj_qty,
            nvl(demand_stock_adj_qty, 0) as demand_stock_adj_qty
        from int_f_wbx_mfg_pct_weekly_pre_agg
    ),
    final as (
        select
            *,
            nvl(
                sum(prev_curr_diff_pa_balance_qty) over (
                    partition by rw_id
                    order by rw_id, week_start_dt, week_end_dt, week_description
                    rows between unbounded preceding and current row
                ),
                0
            ) as sum_prev_curr_diff_pa_balance_qty,

            case
                when rw_id != nvl(prev_rw_id, '')
                then virtual_stock_qty
                else sum_prev_curr_diff_pa_balance_qty + cumulative_virtual_stock_qty
            end as virtual_stock_qty_final
        from pre_final
    )

select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(snapshot_date as date) as snapshot_date,

    cast(
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,

    cast(substring(variant_code, 1, 255) as text(255)) as variant_code,

    cast(substring(source_company_code, 1, 255) as text(255)) as source_company_code,

    cast(substring(plan_version, 1, 255) as text(255)) as plan_version,

    cast(item_guid as text(255)) as item_guid,

    cast(substring(week_description, 1, 255) as text(255)) as week_description,

    cast(week_start_dt as date) as week_start_dt,

    cast(week_end_dt as date) as week_end_dt,

    cast(nvl(week_start_stock_final, 0) as number(38, 10)) as week_start_stock,

    cast(demand_transit_qty as number(38, 10)) as demand_transit_qty,

    cast(supply_transit_qty as number(38, 10)) as supply_transit_qty,

    cast(expired_qty as number(38, 10)) as expired_qty,

    cast(blocked_qty as number(38, 10)) as blocked_qty,

    cast(pa_eff_week_qty as number(38, 10)) as pa_eff_week_qty,

    cast(nvl(pa_balance_qty_final, 0) as number(38, 10)) as pa_balance_qty,

    cast(virtual_stock_qty_final as number(38, 10)) as virtual_stock_qty,

    cast(demand_wo_qty as number(38, 10)) as demand_wo_qty,

    cast(production_wo_qty as number(38, 10)) as production_wo_qty,

    cast(production_planned_wo_qty as number(38, 10)) as production_planned_wo_qty,

    cast(demand_planned_batch_wo_qty as number(38, 10)) as demand_planned_batch_wo_qty,

    cast(
        demand_unplanned_batch_wo_qty as number(38, 10)
    ) as demand_unplanned_batch_wo_qty,

    cast(supply_trans_journal_qty as number(38, 10)) as supply_trans_journal_qty,

    cast(demand_trans_journal_qty as number(38, 10)) as demand_trans_journal_qty,

    cast(demand_planned_trans_qty as number(38, 10)) as demand_planned_trans_qty,

    cast(supply_stock_journal_qty as number(38, 10)) as supply_stock_journal_qty,

    cast(demand_stock_journal_qty as number(38, 10)) as demand_stock_journal_qty,

    cast(supply_po_qty as number(38, 10)) as supply_po_qty,

    cast(supply_planned_po_qty as number(38, 10)) as supply_planned_po_qty,

    cast(supply_po_transfer_qty as number(38, 10)) as supply_po_transfer_qty,

    cast(supply_po_return_qty as number(38, 10)) as supply_po_return_qty,

    cast(sales_order_qty as number(38, 10)) as sales_order_qty,

    cast(return_sales_order_qty as number(38, 10)) as return_sales_order_qty,

    cast(minimum_stock_qty as number(38, 10)) as minimum_stock_qty,

    cast(nvl(week_end_stock_final, 0) as number(38, 10)) as week_end_stock,

    cast(substring(current_week_flag, 1, 20) as text(20)) as current_week_flag,

    cast(load_date as date) as load_date,

    cast(update_date as date) as update_date,

    cast(
        period_end_firm_purchase_qty as number(38, 10)
    ) as period_end_firm_purchase_qty,

    cast(substring(base_currency, 1, 255) as text(255)) as base_currency,

    cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,

    cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,

    cast(oc_base_item_unit_prim_cost as number(38, 10)) as oc_base_item_unit_prim_cost,

    cast(oc_corp_item_unit_prim_cost as number(38, 10)) as oc_corp_item_unit_prim_cost,

    cast(
        oc_pcomp_item_unit_prim_cost as number(38, 10)
    ) as oc_pcomp_item_unit_prim_cost,

    cast(substring(agreement_flag, 1, 20) as text(20)) as agreement_flag,

    cast(supply_stock_adj_qty as number(38, 10)) as supply_stock_adj_qty,

    cast(demand_stock_adj_qty as number(38, 10)) as demand_stock_adj_qty,
    "RW_ID"
from final
