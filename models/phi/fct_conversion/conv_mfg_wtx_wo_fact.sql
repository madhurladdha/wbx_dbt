with source as (

    select * from {{ source('R_EI_SYSADM', 'mfg_wtx_wo_fact') }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'

),

renamed as (

    select
        source_system,
        work_order_number,
        wo_fact_guid,
        source_order_type_code,
        order_type_desc,
        related_document_type,
        related_document_number,
        related_line_number,
        priority_code,
        priority_desc,
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
        actual_start_date,
        actual_completion_date,
        assigned_date,
        source_item_identifier,
        item_guid,
        scheduled_qty,
        cancelled_qty,
        produced_qty,
        transaction_uom,
        source_load_date,
        load_date,
        update_date,
        primary_uom,
        tran_prim_conv_factor,
        tran_lb_conv_factor,
        scheduled_lb_qty,
        produced_lb_qty,
        scheduled_snapshot_date,
        scheduled_snapshot_version,
        orig_planned_completion_date,
        orig_planned_start_date,
        orig_scheduled_qty,
        work_center_code,
        work_center_desc,
        orig_scheduled_kg_qty,
        tran_kg_conv_factor,
        produced_kg_qty,
        scheduled_kg_qty,
        ctp_target_percent,
        ptp_target_percent,
        source_bom_identifier,
        gl_date,
        consolidated_batch_order,
        bulk_flag,
        item_model_group,
        voucher,
        product_class,
        site

    from source

)

select * from renamed
