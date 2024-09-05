{{ config(tags=["wbx", "manufacturing", "work order", "item", "agg"]) }}

with
    mfg_wtx_wo_produced_fact as (
        select * from {{ ref('fct_wbx_mfg_wo_produced') }}
    ),
    mfg_wtx_wo_sched_snapsht_fact as (
        select * from {{ ref('fct_wbx_mfg_wo_sched_snapshot') }}
    ),
    mfg_wtx_plant_wc_weekday_xref as (
        select * from {{ ref("fct_wbx_mfg_plant_wc_weekday_xref") }}
    ),
    src_dim_date as (select * from {{ ref("src_dim_date") }}),
    fct_wbx_mfg_plant_ctp_tgt as (select * from {{ ref("fct_wbx_mfg_plant_ctp_tgt") }}),
    v_mfg_wtx_plant_wc_wkday_xref as (
        select
            source_system,
            source_business_unit_code,
            work_center_code,
            snapshot_day,
            effective_date,
            expiration_date
        from mfg_wtx_plant_wc_weekday_xref
        where
            (
                source_system,
                source_business_unit_code,
                work_center_code,
                snapshot_day,
                version_date,
                version_number
            ) in (
                select
                    source_system,
                    source_business_unit_code,
                    work_center_code,
                    snapshot_day,
                    version_date,
                    version_number
                from
                    (
                        select
                            source_system,
                            source_business_unit_code,
                            work_center_code,
                            snapshot_day,
                            version_date,
                            version_number,
                            rank() over (
                                partition by
                                    source_system,
                                    source_business_unit_code,
                                    work_center_code,
                                    snapshot_day
                                order by version_date desc, version_number desc
                            ) rank_date_version
                        from mfg_wtx_plant_wc_weekday_xref
                    )
                where rank_date_version = 1
            )
    ),
    scheduled as (
        select
            source_system,
            source_item_identifier,
            item_guid,
            source_business_unit_code,
            business_unit_address_guid,
            planned_completion_date as calendar_date,
            sum(scheduled_qty) orig_scheduled_qty,
            sum(scheduled_kg_qty) orig_scheduled_kg_qty,
            work_center_code,
            work_center_desc
        -- from r_ei_sysadm.mfg_wtx_wo_sched_snapsht_fact
        from mfg_wtx_wo_sched_snapsht_fact
        where
            (
                work_order_number,
                source_system,
                source_item_identifier,
                snapshot_date,
                snapshot_version,
                source_business_unit_code
            ) in (
                select
                    work_order_number,
                    source_system,
                    source_item_identifier,
                    snapshot_date,
                    snapshot_version,
                    source_business_unit_code
                from
                    (
                        select
                            f.work_order_number,
                            f.source_system,
                            f.source_item_identifier,
                            f.snapshot_date,
                            f.snapshot_version,
                            f.source_business_unit_code,
                            rank() over (
                                partition by
                                    f.work_order_number,
                                    f.source_system,
                                    f.source_item_identifier,
                                    f.source_business_unit_code
                                order by snapshot_date desc, snapshot_version desc
                            ) rank_date_version
                        -- from r_ei_sysadm.mfg_wtx_wo_sched_snapsht_fact f
                        from mfg_wtx_wo_sched_snapsht_fact f
                        inner join
                            -- r_ei_sysadm.v_mfg_wtx_plant_wc_wkday_xref x
                            v_mfg_wtx_plant_wc_wkday_xref x
                            on f.source_business_unit_code = x.source_business_unit_code
                            and f.work_center_code = x.work_center_code
                            and planned_completion_date
                            between effective_date and expiration_date
                        inner join
                            -- r_ei_sysadm.dim_date d
                            src_dim_date
                            on upper(calendar_day_of_week) = x.snapshot_day
                            and substr(snapshot_date, 1, 10) = calendar_date
                        where snapshot_date <= planned_completion_date
                    )
                where rank_date_version = 1
            )
            and upper(nvl(trim(item_model_group), 'FG-STD')) in ('FG-STD')
        group by
            source_system,
            source_item_identifier,
            item_guid,
            source_business_unit_code,
            business_unit_address_guid,
            planned_completion_date,
            work_center_code,
            work_center_desc
    ),
    produced as (
        select
            source_system,
            source_item_identifier,
            item_guid,
            source_business_unit_code,
            business_unit_address_guid,
            actual_completion_date as calendar_date,
            sum(produced_kg_qty) produced_kg_qty,
            sum(produced_qty) produced_qty,
            nvl(sum(scheduled_qty), 0) orig_scheduled_qty,
            sum(scheduled_kg_qty) orig_scheduled_kg_qty,
            sum(cancelled_qty) cancelled_qty,
            work_center_code,
            work_center_desc,
            sum(produced_lb_qty) as produced_lb_qty
        -- from r_ei_sysadm.mfg_wtx_wo_produced_fact
        from mfg_wtx_wo_produced_fact
        where upper(nvl(trim(item_model_group), 'FG-STD')) in ('FG-STD')
        group by
            source_system,
            source_item_identifier,
            item_guid,
            source_business_unit_code,
            business_unit_address_guid,
            actual_completion_date,
            work_center_code,
            work_center_desc
    ),
    source as (
        select
            nvl(s.source_system, p.source_system) as source_system,
            nvl(
                s.source_item_identifier, p.source_item_identifier
            ) as source_item_identifier,
            nvl(s.item_guid, p.item_guid) as item_guid,
            nvl(
                s.source_business_unit_code, p.source_business_unit_code
            ) as source_business_unit_code,
            nvl(
                s.business_unit_address_guid, p.business_unit_address_guid
            ) as business_unit_address_guid,

            nvl(s.calendar_date, p.calendar_date) as calendar_date,
            nvl(s.work_center_code, p.work_center_code) as work_center_code,
            nvl(s.work_center_desc, p.work_center_desc) as work_center_desc,
            nvl(p.cancelled_qty, 0) as cancelled_qty,
            nvl(p.produced_qty, 0) as produced_qty,
            nvl(p.produced_lb_qty, 0) as produced_lb_qty,
            nvl(p.orig_scheduled_qty, 0) as scheduled_qty,
            nvl(p.orig_scheduled_kg_qty, 0) as scheduled_kg_qty,
            nvl(p.produced_kg_qty, 0) as produced_kg_qty,
            nvl(s.orig_scheduled_kg_qty, 0) as orig_scheduled_kg_qty,
            nvl(s.orig_scheduled_qty, 0) as orig_scheduled_qty
        from scheduled s
        full outer join
            produced p
            on s.calendar_date = p.calendar_date
            and s.source_item_identifier = p.source_item_identifier
            and s.source_business_unit_code = p.source_business_unit_code
            and s.source_system = p.source_system
            and s.work_center_code = p.work_center_code
            and s.work_center_desc = p.work_center_desc
    ),
    final as (
        select src.*, fct.ctp_target, fct.ptp_target
        from source src
        left join
            fct_wbx_mfg_plant_ctp_tgt fct
            on fct.source_business_unit_code = src.source_business_unit_code
            and fct.work_center_code = src.work_center_code
    )

select
    *,
    {{
        dbt_utils.surrogate_key(
            [
                "source_system",
                "source_business_unit_code",
                "source_item_identifier",
                "calendar_date",
                "work_center_code",
            ]
        )
    }} as unique_key
from final
