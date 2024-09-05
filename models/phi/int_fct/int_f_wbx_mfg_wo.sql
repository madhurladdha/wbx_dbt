{{ config(tags=["wbx", "manufacturing", "work order"]) }}

with
    mfg_wtx_wo_produced_fact as (
        select * from {{ ref('fct_wbx_mfg_wo_produced') }}
    ),
    f as (
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
    mfg_wtx_wo_sched_snapsht_fact as (
        select *
        -- from r_ei_sysadm.mfg_wtx_wo_sched_snapsht_fact
        from f
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
                        from f
                        inner join
                            -- r_ei_sysadm.v_mfg_wtx_plant_wc_wkday_xref x
                            v_mfg_wtx_plant_wc_wkday_xref x
                            on f.source_business_unit_code = x.source_business_unit_code
                            and f.work_center_code = x.work_center_code
                            and planned_completion_date
                            between effective_date and expiration_date
                        inner join
                            -- r_ei_sysadm.dim_date d
                            src_dim_date d
                            on upper(calendar_day_of_week) = x.snapshot_day
                            and substr(snapshot_date, 1, 10) = calendar_date
                        where snapshot_date <= planned_completion_date
                    )
                where rank_date_version = 1
            )
    ),
    source as (
        select
            nvl(
                mfg_wtx_wo_produced_fact.source_system,
                mfg_wtx_wo_sched_snapsht_fact.source_system
            ) as source_system,
            nvl(
                mfg_wtx_wo_produced_fact.work_order_number,
                mfg_wtx_wo_sched_snapsht_fact.work_order_number
            ) as work_order_number,
            --mfg_wtx_wo_produced_fact.wo_produced_fact_guid as wo_produced_fact_guid,
            nvl(
                mfg_wtx_wo_produced_fact.source_order_type_code,
                mfg_wtx_wo_sched_snapsht_fact.source_order_type_code
            ) as source_order_type_code,
            nvl(
                mfg_wtx_wo_produced_fact.order_type_desc,
                mfg_wtx_wo_sched_snapsht_fact.order_type_desc
            ) as order_type_desc,
            mfg_wtx_wo_produced_fact.related_document_type as related_document_type,
            mfg_wtx_wo_produced_fact.related_document_number as related_document_number,
            mfg_wtx_wo_produced_fact.related_line_number as related_line_number,
            mfg_wtx_wo_produced_fact.priority_code as priority_code,
            mfg_wtx_wo_produced_fact.priority_desc as priority_desc,
            mfg_wtx_wo_produced_fact.description as description,
            mfg_wtx_wo_produced_fact.company_code as company_code,
            nvl(
                mfg_wtx_wo_produced_fact.source_business_unit_code,
                mfg_wtx_wo_sched_snapsht_fact.source_business_unit_code
            ) as source_business_unit_code,
            nvl(
                mfg_wtx_wo_produced_fact.business_unit_address_guid,
                mfg_wtx_wo_sched_snapsht_fact.business_unit_address_guid
            ) as business_unit_address_guid,
            mfg_wtx_wo_produced_fact.status_code as status_code,
            mfg_wtx_wo_produced_fact.status_desc as status_desc,
            mfg_wtx_wo_produced_fact.status_change_date as status_change_date,
            mfg_wtx_wo_produced_fact.source_customer_code as source_customer_code,
            mfg_wtx_wo_produced_fact.customer_address_number_guid
            as customer_address_number_guid,
            mfg_wtx_wo_produced_fact.wo_creator_add_number as wo_creator_add_number,
            mfg_wtx_wo_produced_fact.manager_add_number as manager_add_number,
            mfg_wtx_wo_produced_fact.supervisor_add_number as supervisor_add_number,
            mfg_wtx_wo_sched_snapsht_fact.planned_completion_date
            as planned_completion_date,
            mfg_wtx_wo_produced_fact.order_date as order_date,
            mfg_wtx_wo_sched_snapsht_fact.planned_start_date as planned_start_date,
            mfg_wtx_wo_produced_fact.requested_date as requested_date,
            mfg_wtx_wo_produced_fact.actual_start_date as actual_start_date,
            mfg_wtx_wo_produced_fact.actual_completion_date as actual_completion_date,
            mfg_wtx_wo_produced_fact.assigned_date as assigned_date,
            nvl(
                mfg_wtx_wo_produced_fact.source_item_identifier,
                mfg_wtx_wo_sched_snapsht_fact.source_item_identifier
            ) as source_item_identifier,
            nvl(
                mfg_wtx_wo_produced_fact.item_guid,
                mfg_wtx_wo_sched_snapsht_fact.item_guid
            ) as item_guid,
            nvl(mfg_wtx_wo_produced_fact.scheduled_qty, 0) as scheduled_qty,
            nvl(mfg_wtx_wo_sched_snapsht_fact.scheduled_qty, 0) as orig_scheduled_qty,
            nvl(mfg_wtx_wo_produced_fact.cancelled_qty, 0) as cancelled_qty,
            nvl(mfg_wtx_wo_produced_fact.produced_qty, 0) as produced_qty,
            mfg_wtx_wo_produced_fact.transaction_uom as transaction_uom,
            mfg_wtx_wo_produced_fact.source_load_date as source_load_date,
            current_date as load_date,
            current_date as update_date,
            mfg_wtx_wo_produced_fact.primary_uom as primary_uom,
            nvl(
                mfg_wtx_wo_produced_fact.tran_prim_conv_factor, 0
            ) as tran_prim_conv_factor,
            nvl(mfg_wtx_wo_produced_fact.tran_lb_conv_factor, 0) as tran_lb_conv_factor,
            nvl(mfg_wtx_wo_produced_fact.scheduled_lb_qty, 0) as scheduled_lb_qty,
            nvl(mfg_wtx_wo_produced_fact.produced_lb_qty, 0) as produced_lb_qty,
            mfg_wtx_wo_sched_snapsht_fact.snapshot_date as snapshot_date,
            mfg_wtx_wo_sched_snapsht_fact.snapshot_version as snapshot_version,
            nvl(
                mfg_wtx_wo_produced_fact.work_center_code,
                mfg_wtx_wo_sched_snapsht_fact.work_center_code
            ) as work_center_code,
            nvl(
                mfg_wtx_wo_produced_fact.work_center_desc,
                mfg_wtx_wo_sched_snapsht_fact.work_center_desc
            ) as work_center_desc,
            nvl(mfg_wtx_wo_produced_fact.scheduled_kg_qty, 0) as scheduled_kg_qty,
            nvl(
                mfg_wtx_wo_sched_snapsht_fact.scheduled_kg_qty, 0
            ) as orig_scheduled_kg_qty,
            nvl(mfg_wtx_wo_produced_fact.tran_kg_conv_factor, 0) as tran_kg_conv_factor,
            nvl(mfg_wtx_wo_produced_fact.produced_kg_qty, 0) as produced_kg_qty,
            nvl(
                mfg_wtx_wo_produced_fact.source_bom_identifier,
                mfg_wtx_wo_sched_snapsht_fact.source_bom_identifier
            ) as source_bom_identifier,
            mfg_wtx_wo_produced_fact.gl_date as gl_date,
            nvl(
                mfg_wtx_wo_produced_fact.consolidated_batch_order,
                mfg_wtx_wo_sched_snapsht_fact.consolidated_batch_order
            ) as consolidated_batch_order,
            nvl(
                mfg_wtx_wo_produced_fact.bulk_flag,
                mfg_wtx_wo_sched_snapsht_fact.bulk_flag
            ) as bulk_flag,
            nvl(
                mfg_wtx_wo_produced_fact.item_model_group,
                mfg_wtx_wo_sched_snapsht_fact.item_model_group
            ) as item_model_group,
            mfg_wtx_wo_produced_fact.voucher as voucher,
            nvl(
                mfg_wtx_wo_produced_fact.product_class,
                mfg_wtx_wo_sched_snapsht_fact.product_class
            ) as product_class,
            nvl(
                mfg_wtx_wo_produced_fact.site, mfg_wtx_wo_sched_snapsht_fact.site
            ) as site

        -- from r_ei_sysadm.mfg_wtx_wo_produced_fact
        from mfg_wtx_wo_produced_fact
        full outer join
            mfg_wtx_wo_sched_snapsht_fact
            on mfg_wtx_wo_produced_fact.source_system
            = mfg_wtx_wo_sched_snapsht_fact.source_system
            and mfg_wtx_wo_produced_fact.work_order_number
            = mfg_wtx_wo_sched_snapsht_fact.work_order_number
            and mfg_wtx_wo_produced_fact.source_system
            = mfg_wtx_wo_sched_snapsht_fact.source_system
            and mfg_wtx_wo_produced_fact.source_item_identifier
            = mfg_wtx_wo_sched_snapsht_fact.source_item_identifier
            and mfg_wtx_wo_produced_fact.source_business_unit_code
            = mfg_wtx_wo_sched_snapsht_fact.source_business_unit_code
            and mfg_wtx_wo_produced_fact.work_center_code
            = mfg_wtx_wo_sched_snapsht_fact.work_center_code
            and mfg_wtx_wo_produced_fact.work_center_desc
            = mfg_wtx_wo_sched_snapsht_fact.work_center_desc
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
                "work_order_number",
            ]
        )
    }} as unique_key
from final
