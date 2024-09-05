{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["manufacturing","batch","wbx"],
    unique_key="UNIQUE_KEY",
    on_schema_change='sync_all_columns', 
    pre_hook="""
    
            {% if check_table_exists( this.schema, this.table ) == 'True' %}
                truncate table {{ this }}
            {% endif %}  

            """,
    )
    
}}

with
    int_fact as (
        select *, row_number() over (partition by unique_key order by 1) rownum
        from {{ ref("int_f_wbx_mfg_batch_order") }}
    )
select
     cast(substring(source_system,1,255) as text(255) ) as source_system  ,

        cast(snapshot_date as date) as snapshot_date  ,

        cast(substring(source_transaction_key,1,255) as text(255) ) as source_transaction_key  ,

        cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

        cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

        cast(substring(variant_code,1,255) as text(255) ) as variant_code  ,

        cast(transaction_eff_date as date) as transaction_eff_date  ,

        cast(substring(transaction_type_code,1,255) as text(255) ) as transaction_type_code  ,

        cast(substring(transaction_desc,1,255) as text(255) ) as transaction_desc  ,

        cast(substring(transaction_status_code,1,255) as text(255) ) as transaction_status_code  ,

        cast(substring(transaction_status_desc,1,255) as text(255) ) as transaction_status_desc  ,

        cast(substring(plan_version,1,255) as text(255) ) as plan_version  ,

        cast(transaction_quantity as number(38,10) ) as transaction_quantity  ,

        cast(transaction_amount as number(38,10) ) as transaction_amount  ,

        cast(substring(reference_text,1,255) as text(255) ) as reference_text  ,

        cast(item_guid as text(255) ) as item_guid  ,

        cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,

        cast(source_updated_date as date) as source_updated_date  ,

        cast(source_updated_time as timestamp_ntz(9) ) as source_updated_time  ,

        cast(substring(source_company,1,255) as text(255) ) as source_company  ,
    
        cast(unique_key as text(255) ) as unique_key   
    from int_fact
    where rownum = 1