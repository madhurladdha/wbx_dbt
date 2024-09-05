{{ config(materialized=env_var("DBT_MAT_VIEW"), tags=["wbx","manufacturing","demand","agg"]) }}

with
    cte_mfg_wtx_demand_comp_agg as (
        select * from {{ ref("fct_wbx_mfg_demand_comp_agg") }}
    ), 
    cte_item as (select * from {{ ref("dim_wbx_item") }}),
    cte_supplier as (select * from (select *,
    ROW_NUMBER() OVER(PARTITION BY SOURCE_SYSTEM_ADDRESS_NUMBER ORDER BY COMPANY_CODE DESC) ROWNUM 
    from {{ ref("dim_wbx_supplier") }}) where ROWNUM=1 ),
    cte_v_inv_wtx_pct_week_snapshot as (
        select * from {{ ref("v_inv_wtx_pct_week_snapshot") }}
    ),
    cte_time_variant_dim as (select * from {{ ref("dim_wbx_mfg_item_variant") }}),
    cte_fct_wbx_mfg_demand_fg_agg as (
        select * from {{ ref("fct_wbx_mfg_demand_fg_agg") }}
    ),
    cte_inv_wtx_daily_balance_fact as (
        select * from {{ ref("fct_wbx_inv_daily_balance") }}
    ),  -- R_EI_SYSADM.inv_wtx_daily_balance_fact
    cte_final as (

        select
            a.source_system,

            a.source_bom_identifier,

            a.week_description,

            a.week_start_dt,

            a.week_end_dt,

            a.plan_version,

            a.wo_src_item_identifier,

            a.wo_variant_code,

            a.wo_src_item_guid,

            a.comp_src_item_identifier,

            a.comp_src_item_guid,

            a.comp_src_variant_code,

            a.comp_required_qty,

            d.item_type as WO_ITEM_TYPE,

            (
                case

                    when (wd.variant_desc is null or trim(wd.variant_desc) = '')

                    then to_char(d.description)

                    else to_char(wd.variant_desc)

                end
            ) wo_description,

            d.buyer_code as wo_buyer_code,

            d.vendor_address_guid as wo_vendor_address_guid,

            d.primary_uom as wo_primary_uom,

            d1.item_type as COMP_ITEM_TYPE,

            (
                case

                    when (cd.variant_desc is null or trim(cd.variant_desc) = '')

                    then to_char(d1.description)

                    else to_char(cd.variant_desc)

                end
            ) comp_description,

            d1.buyer_code as COMP_BUYER_CODE,

            d1.vendor_address_guid as COMP_VENDOR_ADDRESS_GUID,

            d1.primary_uom as COMP_PRIMARY_UOM,

            e.supplier_name,

            null wo_on_hand_qty,

            null wo_planned_qty,

            a.snapshot_date,

            pct.virtual_stock_qty as info_virtual_stock_dontadd,

            a.count_fg_occurence

        from
            (
                select
                    source_system,

                    source_bom_identifier,

                    week_description,

                    week_start_dt,

                    week_end_dt,

                    wo_src_item_identifier,

                    wo_variant_code,

                    wo_src_item_guid,

                    comp_src_item_identifier,

                    plan_version,

                    comp_src_item_guid,

                    comp_src_variant_code,

                    snapshot_date,

                    sum(comp_required_qty) comp_required_qty,

                    avg(count_fg_occurence) count_fg_occurence

                from cte_mfg_wtx_demand_comp_agg

                group by
                    source_system,

                    source_bom_identifier,

                    week_description,

                    week_start_dt,

                    week_end_dt,

                    wo_src_item_identifier,

                    wo_variant_code,

                    wo_src_item_guid,

                    comp_src_item_identifier,

                    plan_version,

                    comp_src_item_guid,

                    comp_src_variant_code,

                    snapshot_date
            ) a

        inner join

            (
                select distinct
                    source_item_identifier,

                    source_system,

                    max(primary_uom) primary_uom,

                    max(item_type) item_type,

                    max(description) description,

                    max(buyer_code) buyer_code,

                    max(vendor_address_guid) vendor_address_guid

                from cte_item

                where source_system = '{{env_var('DBT_SOURCE_SYSTEM')}}'

                group by source_item_identifier, source_system
            ) d

            on a.wo_src_item_identifier = d.source_item_identifier

            and a.source_system = d.source_system

        inner join

            (
                select distinct
                    source_item_identifier,

                    source_system,

                    max(primary_uom) primary_uom,

                    max(item_type) item_type,

                    max(description) description,

                    max(buyer_code) buyer_code,

                    max(vendor_address_guid) vendor_address_guid

                from cte_item

                where source_system = '{{env_var('DBT_SOURCE_SYSTEM')}}'

                group by source_item_identifier, source_system
            ) d1

            on a.comp_src_item_identifier = d1.source_item_identifier

            and a.source_system = d1.source_system

        left join
            cte_supplier e

            on e.source_system = '{{env_var('DBT_SOURCE_SYSTEM')}}'

            and cast(ltrim(e.source_system_address_number, '0') as varchar2(255))
            = cast(d.vendor_address_guid as varchar2(255))

        left join
            cte_v_inv_wtx_pct_week_snapshot pct

            on lower(pct.plan_version) = lower(a.plan_version)

            and pct.snapshot_date = a.snapshot_date

            and pct.source_system = a.source_system

            and pct.week_end_dt = a.week_end_dt

            and pct.week_start_dt = a.week_start_dt

            and pct.variant_code = a.comp_src_variant_code

            and pct.source_item_identifier = a.comp_src_item_identifier

        left join

            (
                select
                    s.source_system,

                    s.source_item_identifier,

                    s.variant_code,

                    s.active_flag,

                    max(s.variant_desc) variant_desc,

                    max(s.item_allocation_key) item_allocation_key

                from
                    cte_time_variant_dim s,

                    (
                        select
                            source_system,

                            source_item_identifier,

                            variant_code,

                            max(active_flag) active_flag

                        from cte_time_variant_dim

                        group by source_system, source_item_identifier, variant_code
                    ) d

                where
                    s.source_item_identifier = d.source_item_identifier

                    and s.variant_code = d.variant_code

                    and s.source_system = d.source_system

                    and s.active_flag = d.active_flag

                group by
                    s.source_system,

                    s.source_item_identifier,

                    s.variant_code,

                    s.active_flag
            ) wd

            on a.source_system = wd.source_system

            and a.wo_src_item_identifier = wd.source_item_identifier

            and a.wo_variant_code = wd.variant_code

        left join

            (
                select
                    s.source_system,

                    s.source_item_identifier,

                    s.variant_code,

                    s.active_flag,

                    max(s.variant_desc) variant_desc,

                    max(s.item_allocation_key) item_allocation_key

                from
                    cte_time_variant_dim s,

                    (
                        select
                            source_system,

                            source_item_identifier,

                            variant_code,

                            max(active_flag) active_flag

                        from cte_time_variant_dim

                        group by source_system, source_item_identifier, variant_code
                    ) d

                where
                    s.source_item_identifier = d.source_item_identifier

                    and s.variant_code = d.variant_code

                    and s.source_system = d.source_system

                    and s.active_flag = d.active_flag

                group by
                    s.source_system,

                    s.source_item_identifier,

                    s.variant_code,

                    s.active_flag
            ) cd

            on a.source_system = cd.source_system

            and a.comp_src_item_identifier = cd.source_item_identifier

            and a.comp_src_variant_code = cd.variant_code

        union

        select
            a.source_system,

            a.source_bom_identifier,

            a.week_description,

            a.week_start_dt,

            a.week_end_dt,

            a.plan_version,

            a.wo_src_item_identifier,

            a.wo_variant_code,

            d.item_guid,

            null comp_src_item_identifier,

            null comp_src_item_guid,

            null comp_src_variant_code,

            null comp_required_qty,

            d.item_type,

            (
                case

                    when (wd.variant_desc is null or trim(wd.variant_desc) = '')

                    then to_char(d.description)

                    else to_char(wd.variant_desc)

                end
            ) wo_description,

            d.buyer_code,

            d.vendor_address_guid,

            d.primary_uom,

            null item_type,

            null comp_description,

            null buyer_code,

            null vendor_address_guid,

            null primary_uom,

            null supplier_name,

            (
                case when week_description = 'BACKLOG' then ib.on_hand_qty else 0 end
            ) wo_on_hand_qty,

            a.wo_planned_qty wo_planned_qty,

            snapshot_date,

            0 as info_virtual_stock_dontadd,

            a.count_fg_occurence

        from
            (
                select
                    source_system,

                    source_bom_identifier,

                    week_description,

                    week_start_dt,

                    week_end_dt,

                    plan_version,

                    wo_src_item_identifier,

                    wo_variant_code,

                    snapshot_date,

                    sum(wo_planned_qty) wo_planned_qty,

                    null count_fg_occurence

                from cte_fct_wbx_mfg_demand_fg_agg

                group by
                    source_system,

                    source_bom_identifier,

                    week_description,

                    week_start_dt,

                    week_end_dt,

                    plan_version,

                    wo_src_item_identifier,

                    wo_variant_code,

                    snapshot_date
            ) a

        inner join

            (
                select distinct
                    source_item_identifier,

                    source_system,

                    item_guid,

                    max(primary_uom) primary_uom,

                    max(item_type) item_type,

                    max(description) description,

                    max(buyer_code) buyer_code,

                    max(vendor_address_guid) vendor_address_guid

                from cte_item

                where source_system = '{{env_var('DBT_SOURCE_SYSTEM')}}'

                group by source_item_identifier, source_system, item_guid
            ) d

            on a.wo_src_item_identifier = d.source_item_identifier

            and a.source_system = d.source_system

        left join

            (
                select
                    source_system,

                    case
                        when trim(variant) = '' or trim(variant) is null
                        then '-'
                        else variant
                    end as variant,

                    source_item_identifier,

                    sum(on_hand_qty) on_hand_qty

                from cte_inv_wtx_daily_balance_fact  -- R_EI_SYSADM.INV_WTX_DAILY_BALANCE_FACT

                where
                    inventory_snapshot_date =

                    (
                        select max(to_date(inventory_snapshot_date))

                        from cte_inv_wtx_daily_balance_fact
                    )

                group by
                    source_system,
                    source_item_identifier,
                    case
                        when trim(variant) = '' or trim(variant) is null
                        then '-'
                        else variant
                    end
            ) ib

            on ib.source_system = a.source_system

            and ib.source_item_identifier = a.wo_src_item_identifier

            and ib.variant = a.wo_variant_code

        left join

            (
                select
                    s.source_system,

                    s.source_item_identifier,

                    s.variant_code,

                    s.active_flag,

                    max(s.variant_desc) variant_desc,

                    max(s.item_allocation_key) item_allocation_key

                from
                    cte_time_variant_dim s,

                    (
                        select
                            source_system,

                            source_item_identifier,

                            variant_code,

                            max(active_flag) active_flag

                        from cte_time_variant_dim

                        group by source_system, source_item_identifier, variant_code
                    ) d

                where
                    s.source_item_identifier = d.source_item_identifier

                    and s.variant_code = d.variant_code

                    and s.source_system = d.source_system

                    and s.active_flag = d.active_flag

                group by
                    s.source_system,

                    s.source_item_identifier,

                    s.variant_code,

                    s.active_flag
            ) wd

            on a.source_system = wd.source_system

            and a.wo_src_item_identifier = wd.source_item_identifier

            and a.wo_variant_code = wd.variant_code
    )
select
    source_system,
    source_bom_identifier,
    week_description,
    week_start_dt,
    week_end_dt,
    plan_version,
    wo_src_item_identifier,
    wo_variant_code,
    wo_src_item_guid,
    comp_src_item_identifier,
    comp_src_item_guid,
    comp_src_variant_code,
    comp_required_qty,
    wo_item_type,
    wo_description,
    wo_buyer_code,
    wo_vendor_address_guid,
    wo_primary_uom,
    comp_item_type,
    comp_description,
    comp_buyer_code,
    comp_vendor_address_guid,
    comp_primary_uom,
    supplier_name,
    wo_on_hand_qty,
    wo_planned_qty,
    snapshot_date,
    info_virtual_stock_dontadd,
    count_fg_occurence
from cte_final
