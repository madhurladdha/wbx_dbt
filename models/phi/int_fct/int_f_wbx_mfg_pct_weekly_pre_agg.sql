{{
    config(
        tags=["wbx", "manufacturing", "percent", "weekly", "agg"],
          materialized=env_var("DBT_MAT_TABLE"),
        transient=true,
         post_hook="""
    
       MERGE INTO {{ this }} new USING
(SELECT NVL(WEEK_END_STOCK,0) WEEK_END_STOCK, SOURCE_SYSTEM,SNAPSHOT_DATE,SOURCE_ITEM_IDENTIFIER,VARIANT_CODE,
SOURCE_COMPANY_CODE,PLAN_VERSION,ITEM_GUID
FROM {{ this }}
WHERE WEEK_DESCRIPTION = 'BACKLOG'
) old
ON old.SOURCE_SYSTEM =new.SOURCE_SYSTEM
AND old.SNAPSHOT_DATE =new.SNAPSHOT_DATE
AND old.SOURCE_ITEM_IDENTIFIER =new.SOURCE_ITEM_IDENTIFIER
AND old.VARIANT_CODE =new.VARIANT_CODE
AND old.SOURCE_COMPANY_CODE =new.SOURCE_COMPANY_CODE
AND old.PLAN_VERSION =new.PLAN_VERSION
AND old.ITEM_GUID =new.ITEM_GUID
AND new.CURRENT_WEEK_FLAG = 'Y'
AND TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp))
WHEN MATCHED THEN UPDATE
SET new.WEEK_START_STOCK = old.WEEK_END_STOCK;

COMMIT;

UPDATE {{ this }} new
SET WEEK_END_STOCK =  NVL((WEEK_START_STOCK+ (SUPPLY_TRANSIT_QTY+ SUPPLY_TRANS_JOURNAL_QTY + SUPPLY_STOCK_JOURNAL_QTY+ SUPPLY_PLANNED_PO_QTY + SUPPLY_PO_QTY+ SUPPLY_PO_TRANSFER_QTY+RETURN_SALES_ORDER_QTY)-( DEMAND_TRANSIT_QTY + DEMAND_WO_QTY + DEMAND_PLANNED_BATCH_WO_QTY+ DEMAND_STOCK_JOURNAL_QTY  + DEMAND_TRANS_JOURNAL_QTY +DEMAND_PLANNED_TRANS_QTY+SUPPLY_PO_RETURN_QTY) -EXPIRED_QTY - BLOCKED_QTY),0)
WHERE new.CURRENT_WEEK_FLAG = 'Y' and  TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp));  

COMMIT;

MERGE INTO {{ this }} new USING
(SELECT NVL(PA_BALANCE_QTY +  WEEK_START_STOCK -BLOCKED_QTY- EXPIRED_QTY- DEMAND_STOCK_ADJ_QTY
+  SUPPLY_STOCK_ADJ_QTY -( DEMAND_PLANNED_BATCH_WO_QTY +  DEMAND_WO_QTY) +( SUPPLY_PLANNED_PO_QTY+ SUPPLY_PO_QTY) ,0) VIRTUAL_STOCK_QTY, SOURCE_SYSTEM,SNAPSHOT_DATE,SOURCE_ITEM_IDENTIFIER,VARIANT_CODE,
SOURCE_COMPANY_CODE,PLAN_VERSION,ITEM_GUID
FROM {{ this }}
WHERE WEEK_DESCRIPTION = 'BACKLOG'
) old
ON old.SOURCE_SYSTEM =new.SOURCE_SYSTEM
AND old.SNAPSHOT_DATE =new.SNAPSHOT_DATE
AND old.SOURCE_ITEM_IDENTIFIER =new.SOURCE_ITEM_IDENTIFIER
AND old.VARIANT_CODE =new.VARIANT_CODE
AND old.SOURCE_COMPANY_CODE =new.SOURCE_COMPANY_CODE
AND old.PLAN_VERSION =new.PLAN_VERSION
AND old.ITEM_GUID =new.ITEM_GUID
AND new.WEEK_DESCRIPTION = 'BACKLOG'
AND TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp))
WHEN MATCHED THEN UPDATE
SET new.VIRTUAL_STOCK_QTY = old.VIRTUAL_STOCK_QTY;

COMMIT;

MERGE INTO {{ this }} new USING
(SELECT NVL(PA_BALANCE_QTY,0) PA_BALANCE_QTY, SOURCE_SYSTEM,SNAPSHOT_DATE,SOURCE_ITEM_IDENTIFIER,VARIANT_CODE,
SOURCE_COMPANY_CODE,PLAN_VERSION,ITEM_GUID
FROM {{ this }}
WHERE WEEK_DESCRIPTION = 'BACKLOG'
) old
ON old.SOURCE_SYSTEM =new.SOURCE_SYSTEM
AND old.SNAPSHOT_DATE =new.SNAPSHOT_DATE
AND old.SOURCE_ITEM_IDENTIFIER =new.SOURCE_ITEM_IDENTIFIER
AND old.VARIANT_CODE =new.VARIANT_CODE
AND old.SOURCE_COMPANY_CODE =new.SOURCE_COMPANY_CODE
AND old.PLAN_VERSION =new.PLAN_VERSION
AND old.ITEM_GUID =new.ITEM_GUID
AND new.CURRENT_WEEK_FLAG = 'Y'
AND TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp))
WHEN MATCHED THEN UPDATE
SET new.PA_BALANCE_QTY = CASE WHEN new.AGREEMENT_FLAG = 'Y' THEN NVL(old.PA_BALANCE_QTY - (new.SUPPLY_PLANNED_PO_QTY + new.SUPPLY_PO_QTY ) + new.PA_EFF_WEEK_QTY ,0) ELSE 0 END;

COMMIT;

MERGE INTO {{ this }} new USING
(SELECT NVL(VIRTUAL_STOCK_QTY,0) - NVL(PA_BALANCE_QTY,0) VIRTUAL_STOCK_QTY, SOURCE_SYSTEM,SNAPSHOT_DATE,SOURCE_ITEM_IDENTIFIER,VARIANT_CODE,
SOURCE_COMPANY_CODE,PLAN_VERSION,ITEM_GUID
FROM {{ this }}
WHERE WEEK_DESCRIPTION = 'BACKLOG'
) old
ON old.SOURCE_SYSTEM =new.SOURCE_SYSTEM
AND old.SNAPSHOT_DATE =new.SNAPSHOT_DATE
AND old.SOURCE_ITEM_IDENTIFIER =new.SOURCE_ITEM_IDENTIFIER
AND old.VARIANT_CODE =new.VARIANT_CODE
AND old.SOURCE_COMPANY_CODE =new.SOURCE_COMPANY_CODE
AND old.PLAN_VERSION =new.PLAN_VERSION
AND old.ITEM_GUID =new.ITEM_GUID
AND new.CURRENT_WEEK_FLAG = 'Y'
AND TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp))
WHEN MATCHED THEN UPDATE
SET new.VIRTUAL_STOCK_QTY = nvl(old.VIRTUAL_STOCK_QTY - (new.DEMAND_PLANNED_BATCH_WO_QTY+new.DEMAND_WO_QTY)  + new.PA_BALANCE_QTY + ( new.SUPPLY_PLANNED_PO_QTY + new.SUPPLY_PO_QTY )- new.DEMAND_STOCK_ADJ_QTY + new.SUPPLY_STOCK_ADJ_QTY - new.EXPIRED_QTY - new.BLOCKED_QTY,0);

COMMIT;
             

            """,
    )
}}

with
    fct_wbx_mfg_supply_sched_dly as (
        select * from {{ ref("fct_wbx_mfg_supply_sched_dly") }}
    ),  -- R_EI_SYSADM.INV_WTX_SUPPLY_SCHED_DLY_FACT
    fct_wbx_mfg_plan_calendar_xref as (
        select * from {{ ref("fct_wbx_mfg_plan_calendar_xref") }}
    ),  -- R_EI_SYSADM.MFG_WTX_PLAN_CALENDAR_XREF
    fct_wbx_prc_agreement as (select * from {{ ref("fct_wbx_prc_agreement") }}),  -- R_EI_SYSADM.PRC_WTX_AGREEMENT_FACT
    fct_wbx_inv_daily_balance as (select * from {{ ref("fct_wbx_inv_daily_balance") }}),  -- R_EI_SYSADM.INV_WTX_DAILY_BALANCE_FACT 

    lkp_itm_cost_dim as (
        select *
        from
            (
                select
                    source_item_identifier as source_item_identifier,
                    base_currency as base_currency,
                    phi_currency as phi_currency,
                    pcomp_currency as pcomp_currency,
                    oc_base_item_unit_prim_cost as oc_base_item_unit_prim_cost,
                    oc_corp_item_unit_prim_cost as oc_corp_item_unit_prim_cost,
                    oc_pcomp_item_unit_prim_cost as oc_pcomp_item_unit_prim_cost,
                    variant_code as variant_code,
                    row_number() over (
                        partition by source_item_identifier, variant_code
                        order by eff_date desc
                    ) as rownum
                from {{ ref("v_dim_wbx_inv_item_cost") }}  -- R_EI_SYSADM.INV_WTX_ITEM_COST_DIM 
                where source_cost_method_code = '07' and expir_date = '2050-12-31'  -- EXPIR_D_ID = 150365 
            )
        where rownum = 1
    ),
    lkp_inv_wtx_supply_sched_dly_fact as (
        select distinct
            source_item_identifier as source_item_identifier,
            coalesce(variant_code, ' ') as variant_code,
            source_company_code as source_company_code,
            plan_version as plan_version,
            item_guid as item_guid,
            sum(onhand_qty) as onhand_qty,
            sum(demand_transit_qty) as demand_transit_qty,
            sum(supply_transit_qty) as supply_transit_qty,
            sum(expired_qty) as expired_qty,
            sum(blocked_qty) as blocked_qty,
            sum(demand_wo_qty) as demand_wo_qty,
            sum(production_wo_qty) as production_wo_qty,
            sum(production_planned_wo_qty) as production_planned_wo_qty,
            sum(demand_planned_batch_wo_qty) as demand_planned_batch_wo_qty,
            sum(prod_planned_batch_wo_qty) as prod_planned_batch_wo_qty,
            sum(supply_trans_journal_qty) as supply_trans_journal_qty,
            sum(demand_trans_journal_qty) as demand_trans_journal_qty,
            sum(demand_planned_trans_qty) as demand_planned_trans_qty,
            sum(supply_stock_journal_qty) as supply_stock_journal_qty,
            sum(demand_stock_journal_qty) as demand_stock_journal_qty,
            sum(supply_planned_po_qty) as supply_planned_po_qty,
            sum(supply_po_qty) as supply_po_qty,
            sum(supply_po_transfer_qty) as supply_po_transfer_qty,
            sum(supply_po_return_qty) as supply_po_return_qty,
            sum(sales_order_qty) as sales_order_qty,
            sum(return_sales_order_qty) as return_sales_order_qty,
            sum(minimum_stock_qty) as minimum_stock_qty,
            'BACKLOG' as week_desc,
            week_start_date as week_start_date,
            week_end_date as week_end_date,
            sum(demand_unplanned_batch_wo_qty) as demand_unplanned_batch_wo_qty
        from fct_wbx_mfg_plan_calendar_xref d, fct_wbx_mfg_supply_sched_dly f
        where
            snapshot_date
            = (select max(snapshot_date) from fct_wbx_mfg_supply_sched_dly)
            and upper(d.week_description) = 'BACKLOG'
            and upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
            and transaction_date < to_date(convert_timezone('UTC', current_timestamp))
            and transaction_date between week_start_date and week_end_date
        group by
            source_item_identifier,
            item_guid,
            coalesce(variant_code, ' '),
            plan_version,
            source_company_code,
            week_description,
            week_start_date,
            week_end_date
        union
        select distinct
            source_item_identifier as source_item_identifier,
            coalesce(variant_code, ' ') as variant_code,
            source_company_code as source_company_code,
            plan_version as plan_version,
            item_guid as item_guid,
            sum(onhand_qty) as onhand_qty,
            sum(demand_transit_qty) as demand_transit_qty,
            sum(supply_transit_qty) as supply_transit_qty,
            sum(expired_qty) as expired_qty,
            sum(blocked_qty) as blocked_qty,
            sum(demand_wo_qty) as demand_wo_qty,
            sum(production_wo_qty) as production_wo_qty,
            sum(production_planned_wo_qty) as production_planned_wo_qty,
            sum(demand_planned_batch_wo_qty) as demand_planned_batch_wo_qty,
            sum(prod_planned_batch_wo_qty) as prod_planned_batch_wo_qty,
            sum(supply_trans_journal_qty) as supply_trans_journal_qty,
            sum(demand_trans_journal_qty) as demand_trans_journal_qty,
            sum(demand_planned_trans_qty) as demand_planned_trans_qty,
            sum(supply_stock_journal_qty) as supply_stock_journal_qty,
            sum(demand_stock_journal_qty) as demand_stock_journal_qty,
            sum(supply_planned_po_qty) as supply_planned_po_qty,
            sum(supply_po_qty) as supply_po_qty,
            sum(supply_po_transfer_qty) as supply_po_transfer_qty,
            sum(supply_po_return_qty) as supply_po_return_qty,
            sum(sales_order_qty) as sales_order_qty,
            sum(return_sales_order_qty) as return_sales_order_qty,
            sum(minimum_stock_qty) as minimum_stock_qty,
            week_description as week_desc,
            week_start_date as week_start_date,
            week_end_date as week_end_date,
            sum(demand_unplanned_batch_wo_qty) as demand_unplanned_batch_wo_qty
        from fct_wbx_mfg_plan_calendar_xref d, fct_wbx_mfg_supply_sched_dly f
        where
            snapshot_date
            = (select max(snapshot_date) from fct_wbx_mfg_supply_sched_dly)
            and transaction_date between week_start_date and week_end_date
            and upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
            and week_description <> 'OUTLOOK'
            and week_description <> 'BACKLOG'
            and transaction_date
            between to_date(convert_timezone('UTC', current_timestamp)) and (
                select max(week_end_date)
                from fct_wbx_mfg_plan_calendar_xref
                where
                    week_description <> 'OUTLOOK'
                    and upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
            )
        group by
            source_item_identifier,
            item_guid,
            coalesce(variant_code, ' '),
            plan_version,
            source_company_code,
            week_description,
            week_start_date,
            week_end_date
    ),

    cte_26wk_supply_1 as (
        select distinct
            b.source_item_identifier,
            b.item_guid,
            coalesce(b.variant_code, ' ') variant_code,
            b.plan_version,
            a.week_description,
            a.week_start_date,
            a.week_end_date,
            b.source_company_code,
            case
                when substr(trim(b.source_item_identifier), 1, 1) = 'P'
                then cast(nvl(trim(b.variant_code), '-') as varchar2(255))
                else '-'
            end as variant_lkp
        from
            (
                select week_description, week_start_date, week_end_date
                from fct_wbx_mfg_plan_calendar_xref
                where
                    upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
                    and week_description <> 'OUTLOOK'
                    and week_description <> 'BACKLOG'
            ) a,
            (
                select distinct
                    source_item_identifier,
                    item_guid,
                    variant_code,
                    plan_version,
                    source_company_code
                from fct_wbx_mfg_supply_sched_dly
                where
                    snapshot_date
                    = (select max(snapshot_date) from fct_wbx_mfg_supply_sched_dly)
                    and transaction_date <= (
                        select max(week_end_date)
                        from fct_wbx_mfg_plan_calendar_xref
                        where
                            week_description <> 'OUTLOOK'
                            and upper(planning_calendar_name)
                            = '26 WEEK SUPPLY SCHEDULE'
                    )
            ) b
    ),

    cte_26wk_supply_2 as (
        select distinct
            b.source_item_identifier,
            b.item_guid,
            coalesce(b.variant_code, ' '),
            b.plan_version,
            'BACKLOG' week_description,
            a.week_start_date,
            a.week_end_date,
            b.source_company_code,
            case
                when substr(trim(b.source_item_identifier), 1, 1) = 'P'
                then cast(nvl(trim(b.variant_code), '-') as varchar2(255))
                else '-'
            end as variant_lkp
        from
            (
                select week_description, week_start_date, week_end_date
                from fct_wbx_mfg_plan_calendar_xref
                where
                    upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
                    and week_description <> 'OUTLOOK'
                    and week_description = 'BACKLOG'
            ) a,
            (
                select distinct
                    source_item_identifier,
                    item_guid,
                    variant_code,
                    plan_version,
                    source_company_code
                from fct_wbx_mfg_supply_sched_dly
                where
                    snapshot_date
                    = (select max(snapshot_date) from fct_wbx_mfg_supply_sched_dly)
                    and transaction_date <= (
                        select max(week_end_date)
                        from fct_wbx_mfg_plan_calendar_xref
                        where
                            week_description <> 'OUTLOOK'
                            and upper(planning_calendar_name)
                            = '26 WEEK SUPPLY SCHEDULE'
                    )
            ) b
    ),

    lkp as (
        select distinct
            source_item_identifier,
            item_guid,
            variant_code,
            'BACKLOG' week_description,
            week_start_date,
            week_end_date,
            sum(
                agreement_quantity - (received_quantity + invoiced_quantity)
            ) pa_eff_week_qty,
            source_company
        from fct_wbx_mfg_plan_calendar_xref d, fct_wbx_prc_agreement f
        where
            status_code = 1
            and upper(d.week_description) = 'BACKLOG'
            and upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
            and agreement_eff_date < to_date(convert_timezone('UTC', current_timestamp))
            and (
                (
                    agreement_type_desc = 'Time based contract'
                    and (
                        agreement_exp_date
                        >= to_date(convert_timezone('UTC', current_timestamp))
                    )
                )
                or (agreement_type_desc = 'Volume based contract')
            )  -- and trim(source_item_identifier) = 'R005425'
        group by
            source_item_identifier,
            item_guid,
            variant_code,
            source_company,
            week_description,
            week_start_date,
            week_end_date
        having sum(agreement_quantity - (received_quantity + invoiced_quantity)) > 0
        union
        select distinct
            source_item_identifier,
            item_guid,
            variant_code,
            week_description,
            week_start_date,
            week_end_date,
            sum(
                agreement_quantity - (received_quantity + invoiced_quantity)
            ) pa_eff_week_qty,
            source_company
        from fct_wbx_mfg_plan_calendar_xref d, fct_wbx_prc_agreement f
        where
            status_code = 1
            and agreement_eff_date between week_start_date and week_end_date
            and upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
            and week_description <> 'OUTLOOK'  -- and trim(source_item_identifier) = 'R005425'
            and agreement_eff_date
            between to_date(convert_timezone('UTC', current_timestamp)) and (
                select max(week_end_date)
                from fct_wbx_mfg_plan_calendar_xref
                where
                    week_description <> 'OUTLOOK'
                    and upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
            )
        group by
            source_item_identifier,
            item_guid,
            variant_code,
            source_company,
            week_description,
            week_start_date,
            week_end_date
    ),

    blk as (
        select
            source_item_identifier as source_item_identifier,
            sum(on_hand_qty) as on_hand_qty,
            variant as variant
        from fct_wbx_inv_daily_balance
        where
            upper(lot_status_desc) = 'BLOCKED'
            and to_date(inventory_snapshot_date) = (
                select to_date(max(inventory_snapshot_date))
                from fct_wbx_inv_daily_balance
            )
        group by source_item_identifier, variant
    ),

    flg as (
        select distinct
            source_item_identifier, variant_code, source_company, 'Y' agreement_flag
        from fct_wbx_prc_agreement
        where
            status_code = 1
            and (
                (
                    (
                        (
                            agreement_type_desc = 'Time based contract'
                            and agreement_exp_date
                            >= to_date(convert_timezone('UTC', current_timestamp))
                        )
                        or (agreement_type_desc = 'Volume based contract')
                    )
                    and agreement_eff_date
                    < to_date(convert_timezone('UTC', current_timestamp))
                    and remain_quantity > 0
                )
                or (
                    agreement_eff_date
                    between to_date(convert_timezone('UTC', current_timestamp)) and (
                        select max(week_end_date)
                        from fct_wbx_mfg_plan_calendar_xref
                        where
                            week_description <> 'OUTLOOK'
                            and upper(planning_calendar_name)
                            = '26 WEEK SUPPLY SCHEDULE'
                    )
                )
            )
    ),

    joined_src as (
        select
            to_date(convert_timezone('UTC', current_timestamp)) as snapshot_date,
            src.source_company_code as source_company_code,
            src.source_item_identifier as source_item_identifier,
            src.variant_code as variant_code,
            src.item_guid as item_guid,
            src.week_description as week_description,
            src.week_start_date as week_start_date,
            src.week_end_date as week_end_date,
            cast(
                case
                    when trim(src.variant_lkp) = '' or trim(src.variant_lkp) is null
                    then '-'
                    else trim(src.variant_lkp)
                end as varchar(255)
            ) as variant_lkp,
            src.plan_version as plan_version,
            nvl(lkp.pa_eff_week_qty, 0) as pa_eff_week_qty,
            case
                when src.week_description = 'BACKLOG' then nvl(on_hand_qty, 0) else 0
            end as blocked_qty,
            nvl(agreement_flag, 'N') as agreement_flag
        from
            (
                select *
                from cte_26wk_supply_1
                union
                select *
                from cte_26wk_supply_2
            ) src
        left join
            lkp
            on src.source_item_identifier = lkp.source_item_identifier
            and src.item_guid = (lkp.item_guid)  -- removed the to_number conversion by ar
            and case
                when trim(src.variant_code) = '' or trim(src.variant_code) is null
                then '-'
                else trim(src.variant_code)
            end = case
                when trim(lkp.variant_code) = '' or trim(lkp.variant_code) is null
                then '-'
                else trim(lkp.variant_code)
            end
            and upper(src.source_company_code) = upper(lkp.source_company)
            and src.week_description = lkp.week_description
            and src.week_start_date = lkp.week_start_date
            and src.week_end_date = lkp.week_end_date
        left join
            blk on

            src.source_item_identifier = blk.source_item_identifier
            and case
                when trim(src.variant_code) = '' or trim(src.variant_code) is null
                then '-'
                else trim(src.variant_code)
            end = case
                when trim(blk.variant) = '' or trim(blk.variant) is null
                then '-'
                else trim(blk.variant)
            end
        left join
            flg
            on src.source_item_identifier = flg.source_item_identifier
            and case
                when trim(src.variant_code) = '' or trim(src.variant_code) is null
                then '-'
                else trim(src.variant_code)
            end = case
                when trim(flg.variant_code) = '' or trim(flg.variant_code) is null
                then '-'
                else trim(flg.variant_code)
            end

            and upper(src.source_company_code) = upper(flg.source_company)
    ),
    final as (
        select
            case
                when
                    (a.snapshot_date) >= (a.week_start_date)
                    and (a.snapshot_date) <= (a.week_end_date)
                then 'Y'
                else 'N'
            end as current_week_flag,
            case
                when a.week_description = 'BACKLOG' then nvl(lkp.onhand_qty, 0) else 0
            end as week_start_stock,
            nvl(lkp.demand_transit_qty, 0) as demand_transit_qty,
            nvl(lkp.supply_transit_qty, 0) as supply_transit_qty,
            nvl(lkp.expired_qty, 0) as expired_qty,
            nvl(lkp.demand_wo_qty, 0) as demand_wo_qty,
            nvl(lkp.production_wo_qty, 0) as production_wo_qty,
            nvl(lkp.production_planned_wo_qty, 0) as production_planned_wo_qty,
            nvl(lkp.demand_planned_batch_wo_qty, 0) as demand_planned_batch_wo_qty,
            nvl(lkp.prod_planned_batch_wo_qty, 0) as prod_planned_batch_wo_qty,
            nvl(lkp.supply_trans_journal_qty, 0) as supply_trans_journal_qty,
            nvl(lkp.demand_trans_journal_qty, 0) as demand_trans_journal_qty,
            nvl(lkp.demand_planned_trans_qty, 0) as demand_planned_trans_qty,
            nvl(lkp.supply_stock_journal_qty, 0) as supply_stock_journal_qty,
            nvl(lkp.demand_stock_journal_qty, 0) as demand_stock_journal_qty,
            nvl(lkp.supply_po_qty, 0) as supply_po_qty,
            nvl(lkp.supply_planned_po_qty, 0) as supply_planned_po_qty,
            nvl(lkp.supply_po_transfer_qty, 0) as supply_po_transfer_qty,
            nvl(lkp.supply_po_return_qty, 0) as supply_po_return_qty,
            nvl(lkp.sales_order_qty, 0) as sales_order_qty,
            nvl(lkp.return_sales_order_qty, 0) as return_sales_order_qty,
            nvl(lkp.minimum_stock_qty, 0) as minimum_stock_qty,
            nvl(
                item_cost_dim.oc_base_item_unit_prim_cost, 0
            ) as oc_base_item_unit_prim_cost,
            item_cost_dim.base_currency,
            item_cost_dim.phi_currency,
            item_cost_dim.pcomp_currency,
            nvl(oc_corp_item_unit_prim_cost, 0) as oc_corp_item_unit_prim_cost,
            nvl(oc_pcomp_item_unit_prim_cost, 0) as oc_pcomp_item_unit_prim_cost,
            nvl(demand_unplanned_batch_wo_qty, 0) as demand_unplanned_batch_wo_qty,
            case
                when
                    a.week_description = 'BACKLOG'
                    and a.agreement_flag = 'Y'
                    then nvl(a.pa_eff_week_qty,0)
                else 0 end as pa_balance_qty,
            supply_transit_qty
            + supply_trans_journal_qty
            + supply_po_transfer_qty
            + supply_stock_journal_qty
            + return_sales_order_qty as supply_stock_adj_qty,
            demand_transit_qty
            + demand_trans_journal_qty
            + demand_planned_trans_qty
            + demand_stock_journal_qty
            + supply_po_return_qty as demand_stock_adj_qty,
            0 as virtual_stock_qty,
            case
                when week_description = 'BACKLOG'
                then
                    (
                        week_start_stock + (
                            supply_transit_qty
                            + supply_trans_journal_qty
                            + supply_stock_journal_qty
                            + supply_planned_po_qty
                            + supply_po_qty
                            + supply_po_transfer_qty
                            + return_sales_order_qty
                        )
                        - (
                            demand_transit_qty
                            + demand_wo_qty
                            + demand_planned_batch_wo_qty
                            + demand_stock_journal_qty
                            + demand_trans_journal_qty
                            + demand_planned_trans_qty
                            + supply_po_return_qty
                        )
                        - expired_qty
                        - a.blocked_qty
                    )
                else 0
            end as week_end_stock,
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "SNAPSHOT_DATE",
                        "a.ITEM_GUID",
                        "a.SOURCE_ITEM_IDENTIFIER",
                        "a.VARIANT_CODE",
                        "a.SOURCE_COMPANY_CODE",
                        "a.PLAN_VERSION",
                    ]
                )
            }} as RW_ID,

            a.*

        from joined_src a
        left join
            lkp_inv_wtx_supply_sched_dly_fact lkp
            on lkp.source_item_identifier = a.source_item_identifier
            and lkp.variant_code = a.variant_code
            and lkp.plan_version = a.plan_version
            and lkp.source_company_code = a.source_company_code
            and lkp.week_desc = a.week_description
            and lkp.week_start_date = a.week_start_date
            and lkp.week_end_date = a.week_end_date
        left join
            lkp_itm_cost_dim item_cost_dim
            on item_cost_dim.source_item_identifier = a.source_item_identifier
            and item_cost_dim.variant_code = a.variant_lkp
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

    cast(week_start_date as date) as week_start_dt,

    cast(week_end_date as date) as week_end_dt,

    cast(nvl(week_start_stock,0) as number(38, 10)) as week_start_stock,

    cast(nvl(demand_transit_qty,0) as number(38, 10)) as demand_transit_qty,

    cast(nvl(supply_transit_qty,0) as number(38, 10)) as supply_transit_qty,

    cast(nvl(expired_qty,0) as number(38, 10)) as expired_qty,

    cast(nvl(blocked_qty,0) as number(38, 10)) as blocked_qty,

    cast(nvl(pa_eff_week_qty,0) as number(38, 10)) as pa_eff_week_qty,

    cast(nvl(pa_balance_qty,0) as number(38, 10)) as pa_balance_qty,

    cast(nvl(virtual_stock_qty,0) as number(38, 10)) as virtual_stock_qty,

    cast(nvl(demand_wo_qty,0) as number(38, 10)) as demand_wo_qty,

    cast(nvl(production_wo_qty,0) as number(38, 10)) as production_wo_qty,

    cast(nvl(production_planned_wo_qty,0) as number(38, 10)) as production_planned_wo_qty,

    cast(nvl(demand_planned_batch_wo_qty,0) as number(38, 10)) as demand_planned_batch_wo_qty,

    cast(
        nvl(demand_unplanned_batch_wo_qty,0) as number(38, 10)
    ) as demand_unplanned_batch_wo_qty,

    cast(nvl(supply_trans_journal_qty,0) as number(38, 10)) as supply_trans_journal_qty,

    cast(nvl(demand_trans_journal_qty,0) as number(38, 10)) as demand_trans_journal_qty,

    cast(nvl(demand_planned_trans_qty,0) as number(38, 10)) as demand_planned_trans_qty,

    cast(nvl(supply_stock_journal_qty,0) as number(38, 10)) as supply_stock_journal_qty,

    cast(nvl(demand_stock_journal_qty,0) as number(38, 10)) as demand_stock_journal_qty,

    cast(nvl(supply_po_qty,0) as number(38, 10)) as supply_po_qty,

    cast(nvl(supply_planned_po_qty,0) as number(38, 10)) as supply_planned_po_qty,

    cast(nvl(supply_po_transfer_qty,0) as number(38, 10)) as supply_po_transfer_qty,

    cast(nvl(supply_po_return_qty,0) as number(38, 10)) as supply_po_return_qty,

    cast(nvl(sales_order_qty,0) as number(38, 10)) as sales_order_qty,

    cast(nvl(return_sales_order_qty,0) as number(38, 10)) as return_sales_order_qty,

    cast(nvl(minimum_stock_qty,0) as number(38, 10)) as minimum_stock_qty,

    cast(nvl(week_end_stock,0) as number(38, 10)) as week_end_stock,

    cast(substring(current_week_flag, 1, 20) as text(20)) as current_week_flag,

    cast(current_date as date) as load_date,

    cast(current_date as date) as update_date,

    cast(0 as number(38, 10)) as period_end_firm_purchase_qty,

    cast(substring(base_currency, 1, 255) as text(255)) as base_currency,

    cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,

    cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,

    cast(oc_base_item_unit_prim_cost as number(38, 10)) as oc_base_item_unit_prim_cost,

    cast(oc_corp_item_unit_prim_cost as number(38, 10)) as oc_corp_item_unit_prim_cost,

    cast(
        oc_pcomp_item_unit_prim_cost as number(38, 10)
    ) as oc_pcomp_item_unit_prim_cost,

    cast(substring(agreement_flag, 1, 20) as text(20)) as agreement_flag,

    cast(nvl(supply_stock_adj_qty,0) as number(38, 10)) as supply_stock_adj_qty,

    cast(nvl(demand_stock_adj_qty,0) as number(38, 10)) as demand_stock_adj_qty,
     cast(RW_ID as text(255) ) as RW_ID

from final
