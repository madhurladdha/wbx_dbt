{{
    config(
        tags=[
            "wbx",
            "manufacturing",
            "demand",
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
    inv_wtx_batch_order_fact as (
        select * from {{ ref("fct_wbx_mfg_batch_order") }}
    ),

    --only select items which are not a parent of another item from BOM
    bi as (
        select x.*
        from mfg_wtx_wo_item_bom_dim x
        left outer join
            mfg_wtx_wo_item_bom_dim y
            on x.source_bom_identifier = y.source_bom_identifier
            and x.comp_src_item_identifier = y.parent_src_item_identifier
            and x.comp_src_variant_code = y.parent_src_variant_code
        where y.source_bom_identifier is null
    ),
    im as (
        select distinct source_item_identifier, item_type
        from itm_item_master_dim
        where source_system = '{{ env_var("DBT_SOURCE_SYSTEM") }}'
    ),

    source as (
        select
            to_date(convert_timezone('UTC', current_timestamp)) as snapshot_date,
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            mwwf.source_bom_identifier,
            c.week_description,
            c.week_start_date as week_start_dt,
            c.week_end_date as week_end_dt,
            'PLANNED' as planned_unplanned_code,
            'Staticplan' as plan_version,
            mwwf.source_item_identifier as wo_src_item_identifier,
            bi.root_src_variant_code as wo_variant_code,
            upper(mwwf.source_business_unit_code) as wo_business_unit_code,
            bi.comp_src_item_identifier,
            upper(mwwf.source_business_unit_code) as comp_business_unit_code,
            bi.comp_src_variant_code,
            sum(
                (mwwf.scheduled_qty) / (bi.root_qty) * (bi.comp_required_qty)
            ) as comp_required_qty
        from mfg_wtx_wo_sched_snapsht_fact mwwf

        inner join
            bi on mwwf.source_bom_identifier = bi.source_bom_identifier
            and mwwf.company_code = bi.root_company_code

        inner join
          im on bi.comp_src_item_identifier = im.source_item_identifier

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
            and im.item_type in ('INGREDIENT', 'PACKAGING')
            and mwwf.planned_completion_date
            > dateadd(day, -7, to_date(convert_timezone('UTC', current_timestamp)))
               --Only select WO's that are at least scheduled but, haven't finished yet
            and (mwwf.status_code between 2 and 4)
            and mwwf.snapshot_version = (
                select max(snapshot_version)
                from mfg_wtx_wo_sched_snapsht_fact
                where
                    snapshot_date = (
                        select max(snapshot_date)
                        from mfg_wtx_wo_sched_snapsht_fact
                    )
            )

        group by
            mwwf.source_bom_identifier,
            c.week_description,
            c.week_start_date,
            c.week_end_date,
            mwwf.source_item_identifier,
            bi.root_src_variant_code,
            mwwf.source_business_unit_code,
            bi.comp_src_item_identifier,
            mwwf.source_business_unit_code,
            bi.comp_src_variant_code
        union all
        -- Batch Works Orders
        select
            to_date(convert_timezone('UTC', current_timestamp)) as snapshot_date,
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            mwwf.reference_text as source_bom_identifier,
            c.week_description,
            c.week_start_date as week_start_dt,
            c.week_end_date as week_end_dt,
            case
                when mwwf.transaction_status_code = 0 then 'UNPLANNED' else 'PLANNED'
            end as planned_unplanned_code,
            mwwf.plan_version,
            mwwf.source_item_identifier as wo_src_item_identifier,
            mwwf.variant_code as wo_variant_code,
            upper(mwwf.source_business_unit_code) as wo_business_unit_code,
            bi.comp_src_item_identifier,
            upper(mwwf.source_business_unit_code) as comp_business_unit_code,
            bi.comp_src_variant_code,
            sum(
                (mwwf.transaction_quantity) / (bi.root_qty) * (bi.comp_required_qty)
            ) as comp_required_qty
        from inv_wtx_batch_order_fact mwwf
        inner join
         bi on mwwf.reference_text = bi.source_bom_identifier
            and mwwf.source_company = bi.root_company_code

        inner join
          im on bi.comp_src_item_identifier = im.source_item_identifier

        inner join
            mfg_wtx_plan_calendar_xref c
            on mwwf.transaction_eff_date between c.week_start_date and c.week_end_date
            and c.planning_calendar_name = '26 WEEK SUPPLY SCHEDULE'
        where
            mwwf.transaction_type_code = 46
            and im.item_type in ('INGREDIENT', 'PACKAGING')
        group by
            mwwf.reference_text,
            c.week_description,
            c.week_start_date,
            c.week_end_date,
            case
                when mwwf.transaction_status_code = 0 then 'UNPLANNED' else 'PLANNED'
            end,
            mwwf.plan_version,
            mwwf.source_item_identifier,
            mwwf.variant_code,
            mwwf.source_business_unit_code,
            bi.comp_src_item_identifier,
            mwwf.source_business_unit_code,
            bi.comp_src_variant_code
    ),

    tfm as (
    select  
            s.snapshot_date,
            s.source_system,
            s.source_bom_identifier,
            s.week_description,
            s.week_start_dt,
            s.week_end_dt,
            s.planned_unplanned_code,
            s.plan_version,
            s.wo_src_item_identifier,
            s.wo_variant_code,
            s.wo_business_unit_code,
            s.comp_src_item_identifier,
            s.comp_business_unit_code,
            s.comp_src_variant_code,
            s.comp_required_qty,
            {{
                dbt_utils.surrogate_key(
                    [
                        "s.source_system",
                        "s.wo_src_item_identifier",
                    ]
                )
            }} as wo_src_item_guid,
           
            {{
                dbt_utils.surrogate_key(
                    [
                        "s.source_system",
                        "s.wo_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }} as wo_business_unit_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "s.source_system",
                        "s.comp_src_item_identifier",
                    ]
                )
            }} as comp_src_item_guid,
            
            {{
                dbt_utils.surrogate_key(
                    [
                        "s.source_system",
                        "s.comp_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }} as comp_business_unit_guid,
            current_timestamp as load_date,
            current_timestamp as update_date,
            0 as count_fg_occurence
    from source s
    
    )

    select 
    cast(snapshot_date as date) as snapshot_date  ,

    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_bom_identifier,1,255) as text(255) ) as source_bom_identifier  ,

    cast(substring(week_description,1,255) as text(255) ) as week_description  ,

    cast(week_start_dt as date) as week_start_dt  ,

    cast(week_end_dt as date) as week_end_dt  ,

    cast(substring(planned_unplanned_code,1,20) as text(20) ) as planned_unplanned_code  ,

    cast(substring(plan_version,1,20) as text(20) ) as plan_version  ,

    cast(substring(wo_src_item_identifier,1,255) as text(255) ) as wo_src_item_identifier  ,

    cast(substring(wo_variant_code,1,255) as text(255) ) as wo_variant_code  ,

    cast(wo_src_item_guid as text(255) ) as wo_src_item_guid  ,

    cast(substring(wo_business_unit_code,1,255) as text(255) ) as wo_business_unit_code  ,

    cast(wo_business_unit_guid as text(255) ) as wo_business_unit_guid  ,

    cast(substring(comp_src_item_identifier,1,255) as text(255) ) as comp_src_item_identifier  ,

    cast(comp_src_item_guid as text(255) ) as comp_src_item_guid  ,

    cast(substring(comp_business_unit_code,1,255) as text(255) ) as comp_business_unit_code  ,

    cast(comp_business_unit_guid as text(255) ) as comp_business_unit_guid  ,

    cast(substring(comp_src_variant_code,1,255) as text(255) ) as comp_src_variant_code  ,

    cast(comp_required_qty as number(38,10) ) as comp_required_qty  ,

    cast(count_fg_occurence as number(38,10) ) as count_fg_occurence  ,

    cast(load_date as date) as load_date  ,

    cast(update_date as date) as update_date  
 
   -- cast(unique_key as text(255) ) as unique_key
    from tfm 