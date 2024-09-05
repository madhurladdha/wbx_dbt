{{
    config(
        tags=["manufacturing", "supply_Schedule", "wbx", "weekly", "inventory"],
        materialized=env_var("DBT_MAT_TABLE"),
        transient=true,
        post_hook="""
        
                {% if check_table_exists( this.schema, this.table ) == 'True' %}
                    UPDATE {{this}} new
SET PERIOD_END_FIRM_PURCHASE_QTY = WEEK_END_STOCK -  SUPPLY_PLANNED_PO_QTY   
WHERE  TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp)) AND new.CURRENT_WEEK_FLAG = 'Y'
                {% endif %}  
    
                """,
    )
}}


with cte_calc as (
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
        ) as rank

    from {{ ref("int_f_wbx_mfg_supply_sched_wkly_pre_agg") }}
    order by rw_id, week_start_dt, week_end_dt, week_description
),

  src_wbx_mfg_agg as (
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
            ) as cumulative_week_end_stock
            
        from cte_calc
    ),

    final as (
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
            source_system,
        snapshot_date,
        source_business_unit_code,
        source_item_identifier,
        variant_code,
        source_company_code,
        source_site_code,
        plan_version,
        item_guid,
        business_unit_address_guid,
        week_description as week_desc,
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
        demand_unplanned_batch_wo_qty
        from src_wbx_mfg_agg
    )

    select cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(snapshot_date as date) as snapshot_date  ,

    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(substring(variant_code,1,255) as text(255) ) as variant_code  ,

    cast(substring(source_company_code,1,255) as text(255) ) as source_company_code  ,

    cast(substring(source_site_code,1,255) as text(255) ) as source_site_code  ,

    cast(substring(plan_version,1,255) as text(255) ) as plan_version  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,

    cast(substring(week_desc,1,255) as text(255) ) as week_desc  ,

    cast(week_start_dt as date) as week_start_dt  ,

    cast(week_end_dt as date) as week_end_dt  ,

    cast(substring(current_week_flag,1,20) as text(20) ) as current_week_flag  ,

    cast(week_start_stock_final as number(38,10) ) as week_start_stock  ,

    cast(demand_transit_qty as number(38,10) ) as demand_transit_qty  ,

    cast(supply_transit_qty as number(38,10) ) as supply_transit_qty  ,

    cast(expired_qty as number(38,10) ) as expired_qty  ,

    cast(blocked_qty as number(38,10) ) as blocked_qty  ,

    cast(demand_wo_qty as number(38,10) ) as demand_wo_qty  ,

    cast(production_wo_qty as number(38,10) ) as production_wo_qty  ,

    cast(production_planned_wo_qty as number(38,10) ) as production_planned_wo_qty  ,

    cast(demand_planned_batch_wo_qty as number(38,10) ) as demand_planned_batch_wo_qty  ,

    cast(prod_planned_batch_wo_qty as number(38,10) ) as prod_planned_batch_wo_qty  ,

    cast(supply_trans_journal_qty as number(38,10) ) as supply_trans_journal_qty  ,

    cast(demand_trans_journal_qty as number(38,10) ) as demand_trans_journal_qty  ,

    cast(demand_planned_trans_qty as number(38,10) ) as demand_planned_trans_qty  ,

    cast(supply_stock_journal_qty as number(38,10) ) as supply_stock_journal_qty  ,

    cast(demand_stock_journal_qty as number(38,10) ) as demand_stock_journal_qty  ,

    cast(supply_po_qty as number(38,10) ) as supply_po_qty  ,

    cast(supply_planned_po_qty as number(38,10) ) as supply_planned_po_qty  ,

    cast(supply_po_transfer_qty as number(38,10) ) as supply_po_transfer_qty  ,

    cast(supply_po_return_qty as number(38,10) ) as supply_po_return_qty  ,

    cast(sales_order_qty as number(38,10) ) as sales_order_qty  ,

    cast(return_sales_order_qty as number(38,10) ) as return_sales_order_qty  ,

    cast(minimum_stock_qty as number(38,10) ) as minimum_stock_qty  ,

    cast(week_end_stock_final as number(38,10) ) as week_end_stock  ,

    cast(period_end_firm_purchase_qty as number(38,10) ) as period_end_firm_purchase_qty  ,

    cast(substring(base_currency,1,30) as text(30) ) as base_currency  ,

    cast(substring(phi_currency,1,30) as text(30) ) as phi_currency  ,

    cast(substring(pcomp_currency,1,30) as text(30) ) as pcomp_currency  ,

    cast(oc_base_item_unit_prim_cost as number(38,10) ) as oc_base_item_unit_prim_cost  ,

    cast(oc_corp_item_unit_prim_cost as number(38,10) ) as oc_corp_item_unit_prim_cost  ,

    cast(oc_pcomp_item_unit_prim_cost as number(38,10) ) as oc_pcomp_item_unit_prim_cost  ,

    cast(demand_unplanned_batch_wo_qty as number(38,10) ) as demand_unplanned_batch_wo_qty,
    RW_ID
     from final
