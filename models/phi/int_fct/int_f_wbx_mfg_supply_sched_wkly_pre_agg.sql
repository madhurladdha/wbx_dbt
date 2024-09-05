{{
    config(
        tags=["manufacturing", "supply_Schedule", "wbx", "weekly", "inventory"],
        materialized=env_var("DBT_MAT_TABLE"),
        transient=true,
        post_hook =    """
        MERGE INTO {{ this }} new USING
(SELECT NVL(WEEK_END_STOCK,0) WEEK_END_STOCK, SOURCE_SYSTEM,SNAPSHOT_DATE,SOURCE_BUSINESS_UNIT_CODE,SOURCE_ITEM_IDENTIFIER,VARIANT_CODE,
SOURCE_COMPANY_CODE,SOURCE_SITE_CODE,PLAN_VERSION,ITEM_GUID,BUSINESS_UNIT_ADDRESS_GUID
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
and old.BUSINESS_UNIT_ADDRESS_GUID=new.BUSINESS_UNIT_ADDRESS_GUID
and old.SOURCE_SITE_CODE=new.SOURCE_SITE_CODE
and old.SOURCE_BUSINESS_UNIT_CODE=new.SOURCE_BUSINESS_UNIT_CODE
AND new.CURRENT_WEEK_FLAG = 'Y'
AND TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp))
WHEN MATCHED THEN UPDATE
SET new.WEEK_START_STOCK = old.WEEK_END_STOCK;

COMMIT;

UPDATE {{ this }} new
SET WEEK_END_STOCK =  NVL((WEEK_START_STOCK+ (SUPPLY_TRANSIT_QTY+ SUPPLY_TRANS_JOURNAL_QTY + SUPPLY_STOCK_JOURNAL_QTY+ SUPPLY_PLANNED_PO_QTY + SUPPLY_PO_QTY+ SUPPLY_PO_TRANSFER_QTY+RETURN_SALES_ORDER_QTY)-( DEMAND_TRANSIT_QTY + DEMAND_WO_QTY + DEMAND_PLANNED_BATCH_WO_QTY+ DEMAND_STOCK_JOURNAL_QTY  + DEMAND_TRANS_JOURNAL_QTY +DEMAND_PLANNED_TRANS_QTY+SUPPLY_PO_RETURN_QTY) -EXPIRED_QTY - BLOCKED_QTY),0)
WHERE new.CURRENT_WEEK_FLAG = 'Y' and  TO_DATE(new.SNAPSHOT_DATE ) = TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp));  

COMMIT;
        """,
    )
}}

with
    fct_wbx_mfg_supply_sched_dly as (
        select * from {{ ref("fct_wbx_mfg_supply_sched_dly") }}
    ),

    dim_wbx_inv_item_cost as (select * from {{ ref("v_dim_wbx_inv_item_cost") }}),

    fct_wbx_inv_daily_balance as (select * from {{ ref("fct_wbx_inv_daily_balance") }}),

    fct_wbx_mfg_plan_calendar_xref as (
        select * from {{ ref("fct_wbx_mfg_plan_calendar_xref") }}
    ),

    lkp as (
        select
            source_item_identifier as source_item_identifier,
            source_business_unit_code as source_business_unit_code,
            sum(on_hand_qty) as on_hand_qty,
            variant as variant
        from fct_wbx_inv_daily_balance
        where
            upper(lot_status_desc) = 'BLOCKED'
            and to_date(inventory_snapshot_date) = (
                select to_date(max(inventory_snapshot_date))
                from fct_wbx_inv_daily_balance
            )
        group by source_item_identifier, source_business_unit_code, variant
    ),

    source_qualifier as (
        select
            to_date(convert_timezone('UTC', current_timestamp)) as snapshot_date,
            s.source_item_identifier as source_item_identifier,
            s.item_guid as item_guid,
            s.source_business_unit_code as source_business_unit_code,
            s.business_unit_address_guid as business_unit_address_guid,
            s.variant_code as variant_code,
            s.source_site_code as source_site_code,
            s.plan_version as plan_version,
            s.week_description as week_description,
            s.week_start_date as week_start_date,
            s.week_end_date as week_end_date,
            s.source_company_code as source_company_code,
            cast(
                case
                    when trim(s.variant_lkp) = '' or trim(s.variant_lkp) is null
                    then '-'
                    else trim(s.variant_lkp)
                end as varchar(255)
            ) as variant_lkp,
            case
                when s.week_description = 'BACKLOG' then nvl(on_hand_qty, 0) else 0
            end as blocked_qty
        from
            (
                (
                    select distinct
                        b.source_item_identifier,
                        b.item_guid,
                        b.source_business_unit_code,
                        b.business_unit_address_guid,
                        b.variant_code,
                        b.source_site_code,
                        b.plan_version,
                        a.week_description,
                        a.week_start_date,
                        a.week_end_date,
                        b.source_company_code,
                        case
                            when substr(trim(source_item_identifier), 1, 1) = 'P'
                            then cast(nvl(trim(variant_code), '-') as varchar2(255))
                            else '-'
                        end as variant_lkp
                    from
                        (
                            select week_description, week_start_date, week_end_date
                            from fct_wbx_mfg_plan_calendar_xref
                            where
                                upper(planning_calendar_name)
                                = '26 WEEK SUPPLY SCHEDULE'
                                and week_description <> 'OUTLOOK'
                                and week_description <> 'BACKLOG'
                        ) a,
                        (
                            select distinct
                                source_item_identifier,
                                item_guid,
                                source_business_unit_code,
                                business_unit_address_guid,
                                variant_code,
                                source_site_code,
                                plan_version,
                                source_company_code
                            from fct_wbx_mfg_supply_sched_dly
                            where
                                snapshot_date = (
                                    select max(snapshot_date)
                                    from fct_wbx_mfg_supply_sched_dly
                                )
                                and transaction_date <= (
                                    select max(week_end_date)
                                    from fct_wbx_mfg_plan_calendar_xref
                                    where
                                        week_description <> 'OUTLOOK'
                                        and upper(planning_calendar_name)
                                        = '26 WEEK SUPPLY SCHEDULE'
                                )
                        ) b
                )
                union
                (
                    select distinct
                        d.source_item_identifier,
                        d.item_guid,
                        d.source_business_unit_code,
                        d.business_unit_address_guid,
                        coalesce(d.variant_code, ' '),
                        d.source_site_code,
                        d.plan_version,
                        c.week_description,
                        c.week_start_date,
                        c.week_end_date,
                        d.source_company_code,
                        case
                            when substr(trim(source_item_identifier), 1, 1) = 'P'
                            then cast(nvl(trim(variant_code), '-') as varchar2(255))
                            else '-'
                        end as variant_lkp
                    from
                        (
                            select week_description, week_start_date, week_end_date
                            from fct_wbx_mfg_plan_calendar_xref
                            where
                                upper(planning_calendar_name)
                                = '26 WEEK SUPPLY SCHEDULE'
                                and week_description <> 'OUTLOOK'
                                and week_description = 'BACKLOG'
                        ) c,
                        (
                            select distinct
                                source_item_identifier,
                                item_guid,
                                source_business_unit_code,
                                business_unit_address_guid,
                                variant_code,
                                source_site_code,
                                plan_version,
                                source_company_code
                            from fct_wbx_mfg_supply_sched_dly
                            where
                                snapshot_date = (
                                    select max(snapshot_date)
                                    from fct_wbx_mfg_supply_sched_dly
                                )
                                and transaction_date <= (
                                    select max(week_end_date)
                                    from fct_wbx_mfg_plan_calendar_xref
                                    where
                                        week_description <> 'OUTLOOK'
                                        and upper(planning_calendar_name)
                                        = '26 WEEK SUPPLY SCHEDULE'
                                )
                        ) d
                )
            ) s
        left join
            lkp
            on s.source_item_identifier = lkp.source_item_identifier
            and s.source_business_unit_code = lkp.source_business_unit_code
            and case
                when trim(s.variant_code) = '' or trim(s.variant_code) is null
                then '-'
                else trim(s.variant_code)
            end = case
                when trim(lkp.variant) = '' or trim(lkp.variant) is null
                then '-'
                else trim(lkp.variant)
            end
    ),

    lkp_wbx_mfg_supply_sched_dly as (
        select
            source_business_unit_code as lkp_source_business_unit_code,
            source_item_identifier as lkp_source_item_identifier,
            variant_code as lkp_variant_code,
            source_company_code as lkp_source_company_code,
            source_site_code as lkp_source_site_code,
            plan_version as lkp_plan_version,
            item_guid as lkp_item_guid,
            business_unit_address_guid as lkp_business_unit_address_guid,
            onhand_qty as lkp_onhand_qty,
            demand_transit_qty as lkp_demand_transit_qty,
            supply_transit_qty as lkp_supply_transit_qty,
            expired_qty as lkp_expired_qty,
            blocked_qty as lkp_blocked_qty,
            demand_wo_qty as lkp_demand_wo_qty,
            production_wo_qty as lkp_production_wo_qty,
            production_planned_wo_qty as lkp_production_planned_wo_qty,
            demand_planned_batch_wo_qty as lkp_demand_planned_batch_wo_qty,
            prod_planned_batch_wo_qty as lkp_prod_planned_batch_wo_qty,
            supply_trans_journal_qty as lkp_supply_trans_journal_qty,
            demand_trans_journal_qty as lkp_demand_trans_journal_qty,
            demand_planned_trans_qty as lkp_demand_planned_trans_qty,
            supply_stock_journal_qty as lkp_supply_stock_journal_qty,
            demand_stock_journal_qty as lkp_demand_stock_journal_qty,
            supply_planned_po_qty as lkp_supply_planned_po_qty,
            supply_po_qty as lkp_supply_po_qty,
            supply_po_transfer_qty as lkp_supply_po_transfer_qty,
            supply_po_return_qty as lkp_supply_po_return_qty,
            sales_order_qty as lkp_sales_order_qty,
            return_sales_order_qty as lkp_return_sales_order_qty,
            minimum_stock_qty as lkp_minimum_stock_qty,
            week_desc as lkp_week_desc,
            week_start_date as lkp_week_start_date,
            week_end_date as lkp_week_end_date,
            demand_unplanned_batch_wo_qty as lkp_demand_unplanned_batch_wo_qty
        from
            (
                select distinct
                    source_business_unit_code as source_business_unit_code,
                    source_item_identifier as source_item_identifier,
                    coalesce(variant_code, ' ') as variant_code,
                    source_company_code as source_company_code,
                    source_site_code as source_site_code,
                    plan_version as plan_version,
                    item_guid as item_guid,
                    business_unit_address_guid as business_unit_address_guid,
                    sum(onhand_qty) as onhand_qty,
                    sum(demand_transit_qty) as demand_transit_qty,
                    sum(supply_transit_qty) as supply_transit_qty,
                    sum(expired_qty) as expired_qty,
                    sum(blocked_qty) as blocked_qty,
                    sum(demand_wo_qty) as demand_wo_qty,
                    sum(production_wo_qty) as production_wo_qty,
                    sum(production_planned_wo_qty) as production_planned_wo_qty,
                    sum(demand_planned_batch_wo_qty) as demand_planned_batch_wo_qty,
                    sum(demand_unplanned_batch_wo_qty) as demand_unplanned_batch_wo_qty,
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
                    week_end_date as week_end_date
                from fct_wbx_mfg_plan_calendar_xref d, fct_wbx_mfg_supply_sched_dly f
                where
                    snapshot_date
                    = (select max(snapshot_date) from fct_wbx_mfg_supply_sched_dly)
                    and upper(d.week_description) = 'BACKLOG'
                    and upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
                    and transaction_date
                    < to_date(convert_timezone('UTC', current_timestamp))
                    and transaction_date between week_start_date and week_end_date
                group by
                    source_item_identifier,
                    item_guid,
                    source_business_unit_code,
                    business_unit_address_guid,
                    coalesce(variant_code, ' '),
                    source_site_code,
                    plan_version,
                    source_company_code,
                    week_description,
                    week_start_date,
                    week_end_date
                union
                select distinct
                    source_business_unit_code as source_business_unit_code,
                    source_item_identifier as source_item_identifier,
                    coalesce(variant_code, ' ') as variant_code,
                    source_company_code as source_company_code,
                    source_site_code as source_site_code,
                    plan_version as plan_version,
                    item_guid as item_guid,
                    business_unit_address_guid as business_unit_address_guid,
                    sum(onhand_qty) as onhand_qty,
                    sum(demand_transit_qty) as demand_transit_qty,
                    sum(supply_transit_qty) as supply_transit_qty,
                    sum(expired_qty) as expired_qty,
                    sum(blocked_qty) as blocked_qty,
                    sum(demand_wo_qty) as demand_wo_qty,
                    sum(production_wo_qty) as production_wo_qty,
                    sum(production_planned_wo_qty) as production_planned_wo_qty,
                    sum(demand_planned_batch_wo_qty) as demand_planned_batch_wo_qty,
                    sum(demand_unplanned_batch_wo_qty) as demand_unplanned_batch_wo_qty,
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
                    week_end_date as week_end_date
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
                            and upper(planning_calendar_name)
                            = '26 WEEK SUPPLY SCHEDULE'
                    )
                group by
                    source_item_identifier,
                    item_guid,
                    source_business_unit_code,
                    business_unit_address_guid,
                    coalesce(variant_code, ' '),
                    source_site_code,
                    plan_version,
                    source_company_code,
                    week_description,
                    week_start_date,
                    week_end_date
            )
    ),

    lkp_wbx_inv_item_cost as (
        select *
        from(
        select 
            source_item_identifier as lkp_item_cost_source_item_identifier,
            source_business_unit_code as lkp_item_cost_source_business_unit_code,
            base_currency as lkp_item_cost_base_currency,
            phi_currency as lkp_item_cost_phi_currency,
            pcomp_currency as lkp_item_cost_pcomp_currency,
            oc_base_item_unit_prim_cost as lkp_item_cost_oc_base_item_unit_prim_cost,
            oc_corp_item_unit_prim_cost as lkp_item_cost_oc_corp_item_unit_prim_cost,
            oc_pcomp_item_unit_prim_cost as lkp_item_cost_oc_pcomp_item_unit_prim_cost,
            variant_code as lkp_item_cost_variant_code,
            row_number() over (
                        partition by source_item_identifier, source_business_unit_code,variant_code
                        order by eff_date desc
                    ) as rownum
        from dim_wbx_inv_item_cost
        where source_cost_method_code = '07' and expir_date = '2050-12-31'
    )
    where rownum =1
    ),

    lookup_tables_join as (
        select *
        from source_qualifier
        left join
            lkp_wbx_mfg_supply_sched_dly
            on lkp_source_business_unit_code = source_business_unit_code
            and lkp_source_item_identifier = source_item_identifier
            and lkp_variant_code = variant_code
            and lkp_plan_version = plan_version
            and lkp_source_company_code = source_company_code
            and lkp_source_site_code = source_site_code
            and lkp_week_desc = week_description
            and lkp_week_start_date = week_start_date
            and lkp_week_end_date = week_end_date
        left join
            lkp_wbx_inv_item_cost
            on lkp_item_cost_source_item_identifier = source_item_identifier
            and lkp_item_cost_source_business_unit_code = source_business_unit_code
            and lkp_item_cost_variant_code = variant_lkp
    ),

    exp_derive_values as (
        select
            *,
            iff(
                date_trunc('DAY', snapshot_date) >= date_trunc('DAY', week_start_date)
                and date_trunc('DAY', snapshot_date)
                <= date_trunc('DAY', week_end_date),
                'Y',
                'N'
            ) as current_week_flag,
            ifnull(
                iff(week_description = 'BACKLOG', lkp_onhand_qty, 0), 0
            ) as week_start_stock,
            ifnull(lkp_demand_transit_qty, 0) as demand_transit_qty,
            ifnull(lkp_supply_transit_qty, 0) as supply_transit_qty,
            ifnull(lkp_expired_qty, 0) as expired_qty,
            ifnull(lkp_demand_wo_qty, 0) as demand_wo_qty,
            ifnull(lkp_production_wo_qty, 0) as production_wo_qty,
            ifnull(lkp_production_planned_wo_qty, 0) as production_planned_wo_qty,
            ifnull(lkp_demand_planned_batch_wo_qty, 0) as demand_planned_batch_wo_qty,
            ifnull(lkp_prod_planned_batch_wo_qty, 0) as prod_planned_batch_wo_qty,
            ifnull(lkp_supply_trans_journal_qty, 0) as supply_trans_journal_qty,
            ifnull(lkp_demand_trans_journal_qty, 0) as demand_trans_journal_qty,
            ifnull(lkp_demand_planned_trans_qty, 0) as demand_planned_trans_qty,
            ifnull(lkp_supply_stock_journal_qty, 0) as supply_stock_journal_qty,
            ifnull(lkp_demand_stock_journal_qty, 0) as demand_stock_journal_qty,
            ifnull(lkp_supply_po_qty, 0) as supply_po_qty,
            ifnull(lkp_supply_planned_po_qty, 0) as supply_planned_po_qty,
            ifnull(lkp_supply_po_transfer_qty, 0) as supply_po_transfer_qty,
            ifnull(lkp_supply_po_return_qty, 0) as supply_po_return_qty,
            ifnull(lkp_sales_order_qty, 0) as sales_order_qty,
            ifnull(lkp_return_sales_order_qty, 0) as return_sales_order_qty,
            ifnull(lkp_minimum_stock_qty, 0) as minimum_stock_qty,
            ifnull(
                lkp_item_cost_oc_base_item_unit_prim_cost, 0
            ) as oc_base_item_unit_prim_cost,
            ifnull(
                lkp_item_cost_oc_corp_item_unit_prim_cost, 0
            ) as oc_corp_item_unit_prim_cost,
            ifnull(
                lkp_item_cost_oc_pcomp_item_unit_prim_cost, 0
            ) as oc_pcomp_item_unit_prim_cost,
            ifnull(
                lkp_demand_unplanned_batch_wo_qty, 0
            ) as demand_unplanned_batch_wo_qty,
            blocked_qty as o_blocked_qty,
            lkp_item_cost_base_currency as base_currency,
            lkp_item_cost_phi_currency as phi_currency,
            lkp_item_cost_pcomp_currency as pcomp_currency
        from lookup_tables_join
    ),

    exp_week_end_stock as (
        select
            exp_derive_values.*,
            ifnull(
                iff(
                    week_description = 'BACKLOG',
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
                        - blocked_qty
                    ),
                    0
                ),
                0
            ) as week_end_stock,
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
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
                        "ITEM_GUID",
                        "BUSINESS_UNIT_ADDRESS_GUID"
                    ]
                )
            }} as rw_id
        from exp_derive_values
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

    cast(substring(source_company_code, 1, 255) as text(255)) as source_company_code,

    cast(substring(source_site_code, 1, 255) as text(255)) as source_site_code,

    cast(substring(plan_version, 1, 255) as text(255)) as plan_version,

    cast(item_guid as text(255)) as item_guid,

    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,

    cast(substring(week_description, 1, 255) as text(255)) as WEEK_DESCRIPTION,

    cast(week_start_date as date) as week_start_dt,

    cast(week_end_date as date) as week_end_dt,

    cast(substring(current_week_flag, 1, 20) as text(20)) as current_week_flag,

    cast(week_start_stock as number(38, 10)) as week_start_stock,

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

    cast(supply_po_qty as number(38, 10)) as supply_po_qty,

    cast(supply_planned_po_qty as number(38, 10)) as supply_planned_po_qty,

    cast(supply_po_transfer_qty as number(38, 10)) as supply_po_transfer_qty,

    cast(supply_po_return_qty as number(38, 10)) as supply_po_return_qty,

    cast(sales_order_qty as number(38, 10)) as sales_order_qty,

    cast(return_sales_order_qty as number(38, 10)) as return_sales_order_qty,

    cast(minimum_stock_qty as number(38, 10)) as minimum_stock_qty,

    cast(week_end_stock as number(38, 10)) as week_end_stock,

    cast(0 as number(38, 10)) as period_end_firm_purchase_qty,

    cast(substring(base_currency, 1, 30) as text(30)) as base_currency,

    cast(substring(phi_currency, 1, 30) as text(30)) as phi_currency,

    cast(substring(pcomp_currency, 1, 30) as text(30)) as pcomp_currency,

    cast(oc_base_item_unit_prim_cost as number(38, 10)) as oc_base_item_unit_prim_cost,

    cast(oc_corp_item_unit_prim_cost as number(38, 10)) as oc_corp_item_unit_prim_cost,

    cast(
        oc_pcomp_item_unit_prim_cost as number(38, 10)
    ) as oc_pcomp_item_unit_prim_cost,

    cast(
        demand_unplanned_batch_wo_qty as number(38, 10)
    ) as demand_unplanned_batch_wo_qty,
    rw_id
from exp_week_end_stock
