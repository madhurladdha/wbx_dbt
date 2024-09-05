with
    old_fct as (
        select *
        from {{ source("FACTS_FOR_COMPARE", "mfg_wtx_wo_produced_fact") }}
        where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    ),
converted_fct as (
    select
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(work_order_number,1,255) as text(255) ) as work_order_number  ,

  --  cast(wo_produced_fact_guid as text(255) ) as wo_produced_fact_guid  ,

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

    cast(actual_start_date as date) as actual_start_date  ,

    cast(actual_completion_date as date) as actual_completion_date  ,

    cast(assigned_date as date) as assigned_date  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(scheduled_qty as number(20,4) ) as scheduled_qty  ,

    cast(cancelled_qty as number(20,4) ) as cancelled_qty  ,

    cast(produced_qty as number(20,4) ) as produced_qty  ,

    cast(substring(transaction_uom,1,255) as text(255) ) as transaction_uom  ,

    cast(source_load_date as date) as source_load_date  ,

    cast(load_date as timestamp_ntz(6) ) as load_date  ,

    cast(update_date as timestamp_ntz(6) ) as update_date  ,

    cast(substring(primary_uom,1,255) as text(255) ) as primary_uom  ,

    cast(tran_prim_conv_factor as number(38,10) ) as tran_prim_conv_factor  ,

    cast(tran_lb_conv_factor as number(38,10) ) as tran_lb_conv_factor  ,

    cast(scheduled_lb_qty as number(38,10) ) as scheduled_lb_qty  ,

    cast(produced_lb_qty as number(38,10) ) as produced_lb_qty  ,

    cast(substring(work_center_code,1,255) as text(255) ) as work_center_code  ,

    cast(substring(work_center_desc,1,255) as text(255) ) as work_center_desc  ,

    cast(scheduled_kg_qty as number(38,10) ) as scheduled_kg_qty  ,

    cast(tran_kg_conv_factor as number(38,10) ) as tran_kg_conv_factor  ,

    cast(produced_kg_qty as number(38,10) ) as produced_kg_qty  ,

    cast(substring(source_bom_identifier,1,255) as text(255) ) as source_bom_identifier  ,

    cast(gl_date as date) as gl_date  ,

    cast(substring(consolidated_batch_order,1,255) as text(255) ) as consolidated_batch_order  ,

    cast(bulk_flag as number(10,0) ) as bulk_flag  ,

    cast(substring(item_model_group,1,255) as text(255) ) as item_model_group  ,

    cast(substring(voucher,1,255) as text(255) ) as voucher  ,

    cast(substring(product_class,1,255) as text(255) ) as product_class  ,

    cast(substring(site,1,255) as text(255) ) as site  ,

    cast(substring(stock_site,1,255) as text(255) ) as stock_site  
 

from old_fct 
)

Select *,                     {{
            dbt_utils.surrogate_key(
                [
                    "source_system",
                    "work_order_number",
                    "work_center_code",
                ]
            )
        }} as unique_key
        
from converted_fct 
   