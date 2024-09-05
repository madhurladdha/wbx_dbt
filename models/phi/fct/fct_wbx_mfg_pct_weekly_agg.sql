{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx", "manufacturing", "percent", "weekly", "agg"],
        unique_key="SNAPSHOT_DATE",
        on_schema_change="sync_all_columns",
        incremental_strategy="delete+insert",
        full_refresh=false,
        post_hook="""
        
                {% if check_table_exists( this.schema, this.table ) == 'True' %}
                    UPDATE {{ this }}  new
    SET PERIOD_END_FIRM_PURCHASE_QTY = WEEK_END_STOCK -  SUPPLY_PLANNED_PO_QTY   
    WHERE  TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp)) AND new.CURRENT_WEEK_FLAG = 'Y'
                {% endif %}  
    
                """,
    )
}}


/* Approach Used: Static Snapshot w/ Historical Conversion
    The approach used for this table is a Snapshot approach but also requires historical conversion from the old IICS data sets.
    Full details can be found in applicable documentation, but the highlights are provided here.
    1) References the old "conversion" or IICS data set for all snapshots up to the migration date.
    2) Environment variables used to drive the filtering so that the IICS data set is only pulled in on the initial run of the model in a new db/env.
    3) Same variables are used to drive filtering on the new (go-forward) data set
    4) End result should be that all old snapshots are captured and then this dbt model appends each new snapshot/version date to the data set in the dbt model.
    Other Design features:
    1) Model should NEVER be allowed to full-refresh.  This could wipe out all history.
    2) Model is incremental with unique_key = version date.  This ensures that past version dates are never deleted and re-runs on the same day will simply delete for
        the given version date and reload.
*/
with
    old_table as (
        select *
        from {{ ref("conv_inv_wtx_pct_wkly_agg") }}
        {% if check_table_exists(this.schema, this.table) == "False" %}
            limit {{ env_var("DBT_NO_LIMIT") }}  -- --------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
        {% else %} limit {{ env_var("DBT_LIMIT") }}  -- ---Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

        {% endif %}

    ),

    base_fct as (
        select *
        from {{ ref("int_f_wbx_mfg_pct_weekly_agg") }}
        {% if check_table_exists(this.schema, this.table) == "True" %}
            limit {{ env_var("DBT_NO_LIMIT") }}
        {% else %} limit {{ env_var("DBT_LIMIT") }}
        {% endif %}
    ),

    old_model as (
        select
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
            supply_stock_adj_qty,
            demand_stock_adj_qty
        from old_table
    ),

    snpt_fact as (
        select
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
            supply_stock_adj_qty,
            demand_stock_adj_qty
        from base_fct

    )

select *
from snpt_fact
union all
select *
from old_model
