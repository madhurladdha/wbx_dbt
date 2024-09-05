{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags = ["wbx","manufacturing","work order","schedule"],
    unique_key='UNIQUE_KEY', 
    on_schema_change='sync_all_columns', 
    incremental_strategy='merge',
    full_refresh=false,
    )
}}



with old_table as (
    select * from {{ref('conv_mfg_wtx_wo_sched_snapsht_fact')}}  
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} 
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}

),

get_snapshot_version as (
    select max(snapshot_version) as snapshot_verion, snapshot_date from 
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
        {{ this }}
    {% else %}
        old_table
    {% endif %}
    group by 2
),

base_fct  as (
    select * from {{ ref ('int_f_wbx_mfg_wo_sched_snapshot') }}
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}
),

old_fact as (
    select 
        cast(substring(source_system,1,255) as text(255) ) as source_system  ,
        cast(substring(work_order_number,1,255) as text(255) ) as work_order_number  ,
        cast(snapshot_date as date) as snapshot_date  ,
        cast(snapshot_version as number(20,0) ) as snapshot_version  ,
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
        {{ dbt_utils.surrogate_key(['source_system','source_business_unit_code',"'PLANT_DC'"]) }} as business_unit_address_guid  ,
        cast(substring(status_code,1,255) as text(255) ) as status_code  ,
        cast(substring(status_desc,1,255) as text(255) ) as status_desc  ,
        cast(status_change_date as date) as status_change_date  ,
        cast(substring(source_customer_code,1,255) as text(255) ) as source_customer_code  ,
        '0' as customer_address_number_guid  ,
        cast(substring(wo_creator_add_number,1,255) as text(255) ) as wo_creator_add_number  ,
        cast(substring(manager_add_number,1,255) as text(255) ) as manager_add_number  ,
        cast(substring(supervisor_add_number,1,255) as text(255) ) as supervisor_add_number  ,
        cast(planned_completion_date as date) as planned_completion_date  ,
        cast(order_date as date) as order_date  ,
        cast(planned_start_date as date) as planned_start_date  ,
        cast(requested_date as date) as requested_date  ,
        cast(assigned_date as date) as assigned_date  ,
        cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
        {{ dbt_utils.surrogate_key(['source_system','source_item_identifier']) }} as item_guid  ,
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
    from old_table

),

snpt_fact as (
    select
        source_system,
        work_order_number,
        base_fct.snapshot_date,
        coalesce((get_snapshot_version.snapshot_verion+1),base_fct.snapshot_version) as snapshot_version,
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
        assigned_date,
        source_item_identifier,
        item_guid,
        scheduled_qty,
        transaction_uom,
        load_date,
        update_date,
        primary_uom,
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
    from base_fct
    left join get_snapshot_version
    on base_fct.snapshot_date = get_snapshot_version.snapshot_date

),

fact as (
    select * from snpt_fact
    union
    select * from old_fact
),

final as (
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
    from fact
)

select * from final

    