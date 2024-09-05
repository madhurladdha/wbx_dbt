{{
    config(
        tags=[
            "wbx",
            "manufacturing",
            "demand",
            "fg",
            "agg",
        ]
    )
}}

with
    mfg_wtx_wo_sched_snapsht_fact as (
        select * from {{ ref("fct_wbx_mfg_wo_sched_snapshot") }}
    ),
    mfg_wtx_wo_item_bom_dim as (select * from {{ ref("dim_wbx_mfg_wo_item_bom") }}),
    itm_item_master_dim as (select * from {{ ref("dim_wbx_item") }}),
    mfg_wtx_plan_calendar_xref as (
        select * from {{ ref("fct_wbx_mfg_plan_calendar_xref") }}
    ),
    inv_wtx_batch_order_fact as (select * from {{ ref("fct_wbx_mfg_batch_order") }}),
    inv_wtx_daily_balance_fact as (
        select * from {{ ref("fct_wbx_inv_daily_balance") }}
    ),
    bi as (
        select distinct
            x.source_bom_identifier, x.root_src_variant_code, x.root_company_code
        from mfg_wtx_wo_item_bom_dim x
        left outer join
            mfg_wtx_wo_item_bom_dim y
            on x.source_bom_identifier = y.source_bom_identifier
            and x.comp_src_item_identifier = y.parent_src_item_identifier
            and x.comp_src_variant_code = y.parent_src_variant_code
        where y.source_bom_identifier is null
    ),

    mwwf as (
        select
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            mwwf.source_bom_identifier,
            'StaticPlan' as plan_version,
            mwwf.source_item_identifier as wo_src_item_identifier,
            bi.root_src_variant_code as wo_variant_code,
            planned_completion_date as transaction_date,
            'PLANNED' as planned_unplanned_code,
            upper(mwwf.source_business_unit_code) as wo_business_unit_code
        from mfg_wtx_wo_sched_snapsht_fact mwwf
        inner join
            bi
            on mwwf.source_bom_identifier = bi.source_bom_identifier
            and mwwf.company_code = bi.root_company_code
        where
            mwwf.snapshot_date
            = (select max(snapshot_date) from mfg_wtx_wo_sched_snapsht_fact)
            and mwwf.planned_completion_date
            > to_date(convert_timezone('UTC', current_timestamp)) - 7
            and (mwwf.status_code between 2 and 4)
            and mwwf.snapshot_version = (
                select max(snapshot_version)
                from mfg_wtx_wo_sched_snapsht_fact
                where
                    snapshot_date
                    = (select max(snapshot_date) from mfg_wtx_wo_sched_snapsht_fact)
            )
        union
        select
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            mwwf.reference_text as source_bom_identifier,
            mwwf.plan_version,
            mwwf.source_item_identifier as wo_src_item_identifier,
            mwwf.variant_code as wo_variant_code,
            transaction_eff_date as transaction_date,
            case
                when mwwf.transaction_status_code = 0 then 'UNPLANNED' else 'PLANNED'
            end as planned_unplanned_code,
            upper(mwwf.source_business_unit_code) as wo_business_unit_code
        from inv_wtx_batch_order_fact mwwf
        where mwwf.transaction_type_code = 46
    ),

    tfm as (
        select distinct
            mwwf.source_system,
            source_bom_identifier,
            week_description,
            week_start_date,
            week_end_date,
            plan_version,
            planned_unplanned_code,
            wo_src_item_identifier,
            wo_business_unit_code,
            case
                when trim(wo_variant_code) = '' or trim(wo_variant_code) is null
                then '-'
                else wo_variant_code
            end wo_variant_code,
            0 wo_on_hand_qty,
            0 wo_planned_qty,
            to_date(convert_timezone('UTC', current_timestamp)) snapshot_date
        from mwwf
        inner join
            mfg_wtx_plan_calendar_xref c
            /* For all weeks*/
            on upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
            and week_description <> 'OUTLOOK'
            and (
                case
                    when
                        transaction_date
                        < to_date(convert_timezone('UTC', current_timestamp))
                    then to_date(convert_timezone('UTC', current_timestamp))
                    else transaction_date
                end
            ) between to_date(convert_timezone('UTC', current_timestamp)) and (
                select max(week_end_date)
                from mfg_wtx_plan_calendar_xref
                where
                    week_description <> 'OUTLOOK'
                    and upper(planning_calendar_name) = '26 WEEK SUPPLY SCHEDULE'
            )
    ),
    ulkp_mfg_wtx_wo_sched_snapsht_fact as (
        select
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            mwwf.source_bom_identifier,
            c.week_description,
            c.week_start_date as week_start_dt,
            c.week_end_date as week_end_dt,
            'StaticPlan' as plan_version,
            mwwf.source_item_identifier as wo_src_item_identifier,
            bi.root_src_variant_code as wo_variant_code,
            'PLANNED' as planned_unplanned_code,
            upper(mwwf.source_business_unit_code) as wo_business_unit_code,
            sum(cast(mwwf.scheduled_qty as float)) as wo_planned_qty
        from mfg_wtx_wo_sched_snapsht_fact mwwf
        inner join
            bi
            on mwwf.source_bom_identifier = bi.source_bom_identifier
            and mwwf.company_code = bi.root_company_code
        inner join
            mfg_wtx_plan_calendar_xref c
            on case
                when
                    mwwf.planned_completion_date
                    < to_date(convert_timezone('UTC', current_timestamp))
                then to_date(convert_timezone('UTC', current_timestamp))
                else mwwf.planned_completion_date
            end
            between c.week_start_date and c.week_end_date
            and c.planning_calendar_name = '26 WEEK SUPPLY SCHEDULE'
        where
            mwwf.snapshot_date
            = (select max(snapshot_date) from mfg_wtx_wo_sched_snapsht_fact)
            and mwwf.planned_completion_date
            > to_date(convert_timezone('UTC', current_timestamp)) - 7
            and (mwwf.status_code between 2 and 4)
            and mwwf.snapshot_version = (
                select max(snapshot_version)
                from mfg_wtx_wo_sched_snapsht_fact
                where
                    snapshot_date
                    = (select max(snapshot_date) from mfg_wtx_wo_sched_snapsht_fact)
            )

        group by
            mwwf.source_bom_identifier,
            c.week_description,
            c.week_start_date,
            c.week_end_date,
            mwwf.source_item_identifier,
            bi.root_src_variant_code,
            upper(mwwf.source_business_unit_code)
        union all
        -- Batch Works Orders
        select
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            mwwf.reference_text as source_bom_identifier,
            c.week_description,
            c.week_start_date as week_start_dt,
            c.week_end_date as week_end_dt,
            mwwf.plan_version,
            mwwf.source_item_identifier as wo_src_item_identifier,
            mwwf.variant_code as wo_variant_code,
            case
                when mwwf.transaction_status_code = 0 then 'UNPLANNED' else 'PLANNED'
            end as planned_unplanned_code,
            upper(mwwf.source_business_unit_code) as wo_business_unit_code,
            sum(cast(mwwf.transaction_quantity as float)) as wo_planned_qty
        from inv_wtx_batch_order_fact mwwf
        inner join
            mfg_wtx_plan_calendar_xref c
            on mwwf.transaction_eff_date between c.week_start_date and c.week_end_date
            and c.planning_calendar_name = '26 WEEK SUPPLY SCHEDULE'
        where mwwf.transaction_type_code = 46
        group by
            mwwf.reference_text,
            c.week_description,
            c.week_start_date,
            c.week_end_date,
            mwwf.plan_version,
            mwwf.source_item_identifier,
            mwwf.variant_code,
            case
                when mwwf.transaction_status_code = 0 then 'UNPLANNED' else 'PLANNED'
            end,
            upper(mwwf.source_business_unit_code)
    ),

    lkp_snapshot_fct as (
        select
            source_system,
            source_bom_identifier,
            week_description,
            week_start_dt as week_start_date,
            week_end_dt as week_end_date,
            plan_version,
            wo_src_item_identifier,
            wo_variant_code,
            planned_unplanned_code,
            wo_business_unit_code,
            to_date(convert_timezone('UTC',current_timestamp))  as  snapshot_date,
            wo_planned_qty
        from ulkp_mfg_wtx_wo_sched_snapsht_fact

    ),
    ulkp_inv_wtx_daily_balance_fact as (
        select
            source_item_identifier as source_item_identifier,
            source_business_unit_code as source_business_unit_code,
            sum(on_hand_qty) as on_hand_qty,
            source_system as source_system,
            case
                when trim(variant) = '' or trim(variant) is null then '-' else variant
            end as variant
        from inv_wtx_daily_balance_fact
        where
            inventory_snapshot_date = (
                select max(to_date(inventory_snapshot_date))
                from inv_wtx_daily_balance_fact
            )
        group by
            source_system, source_item_identifier, source_business_unit_code, variant
    ),
    source as (
        select
            t.source_system,
            t.snapshot_date,
            t.source_bom_identifier,
            t.week_description,
            t.week_start_date as week_start_dt,
            t.week_end_date as week_end_dt,
            s.week_start_date,
            s.week_end_date,
            t.planned_unplanned_code,
            t.plan_version,
            t.wo_src_item_identifier,
            t.wo_variant_code,
            {{
                dbt_utils.surrogate_key(
                    [
                        "t.source_system",
                        "t.wo_src_item_identifier",
                    ]
                )
            }} as wo_src_item_guid,
            t.wo_business_unit_code,
            {{
                dbt_utils.surrogate_key(
                    [
                        "t.source_system",
                        "t.wo_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }} as wo_business_unit_guid,
            case when s.wo_planned_qty is null then 0 else s.wo_planned_qty END  wo_planned_qty,
            case when d.on_hand_qty is null  Then 0 Else d.on_hand_qty end wo_on_hand_qty,
       
            current_timestamp() as update_date
        from tfm t
        left outer join
            lkp_snapshot_fct s
            on s.source_system = t.source_system
            and s.source_bom_identifier = t.source_bom_identifier
            and s.week_description = t.week_description
            and s.week_start_date = t.week_start_date
            and s.week_end_date = t.week_end_date
            and s.wo_src_item_identifier = t.wo_src_item_identifier
            and s.wo_src_item_identifier = t.wo_src_item_identifier
            and s.wo_business_unit_code =  t.wo_business_unit_code
            and s.plan_version = t.plan_version
            and s.snapshot_date = t.snapshot_date
        left outer join
            ulkp_inv_wtx_daily_balance_fact d
            on d.source_item_identifier = t.wo_src_item_identifier
            and d.source_business_unit_code = t.wo_business_unit_code
            and d.variant = t.wo_variant_code

    )
select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(snapshot_date as date) as snapshot_date,

    cast(
        substring(source_bom_identifier, 1, 255) as text(255)
    ) as source_bom_identifier,

    cast(substring(week_description, 1, 255) as text(255)) as week_description,

    cast(week_start_dt as date) as week_start_dt,

    cast(week_end_dt as date) as week_end_dt,

    cast(
        substring(planned_unplanned_code, 1, 20) as text(20)
    ) as planned_unplanned_code,

    cast(substring(plan_version, 1, 20) as text(20)) as plan_version,

    cast(
        substring(wo_src_item_identifier, 1, 255) as text(255)
    ) as wo_src_item_identifier,

    cast(substring(wo_variant_code, 1, 255) as text(255)) as wo_variant_code,

    cast(wo_src_item_guid as text(255)) as wo_src_item_guid,

    cast(
        substring(wo_business_unit_code, 1, 255) as text(255)
    ) as wo_business_unit_code,

    cast(wo_business_unit_guid as text(255)) as wo_business_unit_guid,

    cast(wo_planned_qty as number(38, 10)) as wo_planned_qty,

   cast(wo_on_hand_qty as number(38, 10)) as wo_on_hand_qty,


    cast(update_date as date) as update_date

-- cast(unique_key as text(255) ) as unique_key 
from source
