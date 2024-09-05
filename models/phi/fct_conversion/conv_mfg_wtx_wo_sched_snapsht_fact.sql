{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    query_tag='test_conversion',
    )
}}

WITH old_fct AS 
(
    SELECT * FROM {{source('FACTS_FOR_COMPARE','mfg_wtx_wo_sched_snapsht_fact')}} WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

converted_fct as (
    select
        source_system,
        work_order_number,
        snapshot_date,
        snapshot_version,
        wo_sched_snapsht_guid,
        source_order_type_code,
        order_type_desc,
        related_document_type,
        related_document_number,
        related_line_number,
        priority_code,
        priority_desc as priority_desc,
        description,
        company_code,
        source_business_unit_code,
        business_unit_address_guid,
        status_code,
        status_desc,
        status_change_date,
        source_customer_code,
        customer_address_number_guid,
        wo_creator_add_number,
        manager_add_number,
        supervisor_add_number,
        planned_completion_date,
        order_date,
        planned_start_date,
        requested_date,
        assigned_date,
        source_item_identifier,
        item_guid,
        scheduled_qty,
        transaction_uom,
        load_date,
        update_date,
        upper(primary_uom) as primary_uom,
        tran_prim_conv_factor,
        tran_lb_conv_factor,
        scheduled_lb_qty,
        work_center_code,
        work_center_desc,
        scheduled_kg_qty,
        tran_kg_conv_factor,
        source_bom_identifier,
        gl_date,
        consolidated_batch_order,
        bulk_flag,
        item_model_group,
        voucher,
        product_class,
        site
    from old_fct 
    )

select
    *,
    {{
        dbt_utils.surrogate_key(
            [
                "cast(substring(source_system,1,255) as text(255) )",
                "cast(substring(work_order_number,1,255) as text(255) )",
                "cast(snapshot_date as date)",
                "cast(snapshot_version as number(20,0) )",
                "cast(substring(work_center_code,1,255) as text(255) )"
            ]
        )
    }} as unique_key
from converted_fct