

{{
    config(
        tags = ["wbx","manufacturing","work order","schedule"]
    )
}}

with stage_table as (
    select * from {{ ref('stg_f_wbx_mfg_wo_sched_snapshot') }}
),

source as (
    select 
        snapshot_date,
        planned_start_date,
        planned_completion_date,
        source_order_type_code,
        order_type_desc,
        work_order_number,
        related_document_type,
        related_document_number,
        related_line_number,
        priority_code,
        priority_desc,
        description,
        upper(company_code) as company_code,
        source_business_unit_code,
        status_code,
        status_desc,
        status_change_date,
        customer_address_number,
        originator_add_number as wo_creator_add_number,
        manager_add_number,
        supervisor_add_number,
        order_date,
        requested_date,
        assigned_date,
        source_item_identifier,
        scheduled_qty,
        transaction_uom,
        source_load_date,
        source_system,
        load_date,
        update_date,
        work_center_code,
        work_center_desc,
        upper(primary_uom) as primary_uom,
        tran_prim_conv_factor,
        tran_lb_conv_factor,
        scheduled_lb_qty,
        scheduled_kg_qty,
        tran_kg_conv_factor,
        source_bom_identifier,
        consolidated_batch_order,
        voucher,
        item_model_group,
        product_class,
        site,
        gl_date,
        bulk_flag
    from stage_table
        /* For D365, the enumitemlabel values are not populated on 10-May-2024.  But the enumitemname is very similar so then defaulting to that.
        This value list in this filter is updated to account for either value coming through.
            STARTED = STARTEDUP
            RELEASED = RELEASED
            SCHEDULED = SCHEDULED
       */
    where upper(status_desc) in ('STARTED','STARTEDUP','RELEASED','SCHEDULED')
),

trans_source as (
    select 
        source.*,
        null as source_customer_code,
        '0' as customer_address_number_guid,
        {{ dbt_utils.surrogate_key(['source_system','source_item_identifier']) }} as item_guid,
        {{ dbt_utils.surrogate_key(['source_system','source_business_unit_code',"'PLANT_DC'"]) }} as business_unit_address_guid
    from source
),


final as (
    select 
        cast(substring(source_system,1,255) as text(255) ) as source_system  ,
        cast(substring(work_order_number,1,255) as text(255) ) as work_order_number  ,
        cast(snapshot_date as date) as snapshot_date  ,
        1 as snapshot_version  ,
        cast(substring(source_order_type_code,1,255) as text(255) ) as source_order_type_code  ,
        cast(substring(order_type_desc,1,255) as text(255) ) as order_type_desc  ,
        cast(substring(related_document_type,1,255) as text(255) ) as related_document_type  ,
        cast(substring(related_document_number,1,255) as text(255) ) as related_document_number  ,
        cast(substring(related_line_number,1,255) as text(255) ) as related_line_number  ,
        cast(substring(priority_code,1,255) as text(255) ) as priority_code  ,
        cast(substring(priority_desc,1,255) as text(255) ) as priority_desc  ,
        cast(substring(description,1,255) as text(255) ) as description  ,
        cast(substring(company_code,1,255) as text(255) ) as company_code  ,
        cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
        cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,
        cast(substring(status_code,1,255) as text(255) ) as status_code  ,
        cast(substring(status_desc,1,255) as text(255) ) as status_desc  ,
        cast(status_change_date as date) as status_change_date  ,
        cast(substring(source_customer_code,1,255) as text(255) ) as source_customer_code  ,
        cast(customer_address_number_guid as text(255) ) as customer_address_number_guid  ,
        cast(substring(wo_creator_add_number,1,255) as text(255) ) as wo_creator_add_number  ,
        cast(substring(manager_add_number,1,255) as text(255) ) as manager_add_number  ,
        cast(substring(supervisor_add_number,1,255) as text(255) ) as supervisor_add_number  ,
        cast(planned_completion_date as date) as planned_completion_date  ,
        cast(order_date as date) as order_date  ,
        cast(planned_start_date as date) as planned_start_date  ,
        cast(requested_date as date) as requested_date  ,
        cast(assigned_date as date) as assigned_date  ,
        cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
        cast(item_guid as text(255) ) as item_guid  ,
        cast(scheduled_qty as number(20,4) ) as scheduled_qty  ,
        cast(substring(transaction_uom,1,255) as text(255) ) as transaction_uom  ,
        cast(load_date as timestamp_ntz(6) ) as load_date  ,
        cast(update_date as timestamp_ntz(6) ) as update_date  ,
        cast(substring(primary_uom,1,255) as text(255) ) as primary_uom  ,
        cast(tran_prim_conv_factor as number(38,10) ) as tran_prim_conv_factor  ,
        cast(tran_lb_conv_factor as number(38,10) ) as tran_lb_conv_factor  ,
        cast(scheduled_lb_qty as number(38,10) ) as scheduled_lb_qty  ,
        cast(substring(work_center_code,1,255) as text(255) ) as work_center_code  ,
        cast(substring(work_center_desc,1,255) as text(255) ) as work_center_desc  ,
        cast(scheduled_kg_qty as number(38,10) ) as scheduled_kg_qty  ,
        cast(tran_kg_conv_factor as number(38,10) ) as tran_kg_conv_factor  ,
        cast(substring(source_bom_identifier,1,255) as text(255) ) as source_bom_identifier  ,
        cast(gl_date as date) as gl_date  ,
        cast(substring(consolidated_batch_order,1,255) as text(255) ) as consolidated_batch_order  ,
        cast(bulk_flag as number(10,0) ) as bulk_flag  ,
        cast(substring(item_model_group,1,255) as text(255) ) as item_model_group  ,
        cast(substring(voucher,1,255) as text(255) ) as voucher  ,
        cast(substring(product_class,1,255) as text(255) ) as product_class  ,
        cast(substring(site,1,255) as text(255) ) as site 
    from trans_source
)

select * from final