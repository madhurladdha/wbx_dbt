{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["manufacturing","agreement","wbx"],
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
        from {{ ref("int_f_wbx_prc_agreement") }}
    )
select
        cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(agreement_number,1,255) as text(255) ) as agreement_number  ,

    cast(line_number as number(38,0) ) as line_number  ,

    cast(substring(agreement_type_desc,1,255) as text(255) ) as agreement_type_desc  ,

    cast(substring(source_company,1,255) as text(255) ) as source_company  ,

    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(substring(variant_code,1,255) as text(255) ) as variant_code  ,

    cast(substring(site_code,1,255) as text(255) ) as site_code  ,

    cast(substring(status_code,1,255) as text(255) ) as status_code  ,

    cast(substring(status_desc,1,255) as text(255) ) as status_desc  ,

    cast(substring(approval_status_code,1,255) as text(255) ) as approval_status_code  ,

    cast(substring(approval_status_desc,1,255) as text(255) ) as approval_status_desc  ,

    cast(agreement_eff_date as date) as agreement_eff_date  ,

    cast(agreement_exp_date as date) as agreement_exp_date  ,

    cast(substring(supplier_address_number,1,255) as text(255) ) as supplier_address_number  ,

    cast(agreement_quantity as number(38,10) ) as agreement_quantity  ,

    cast(original_quantity as number(38,10) ) as original_quantity  ,

    cast(price_per_unit as number(38,10) ) as price_per_unit  ,

    cast(substring(unit_of_measure,1,255) as text(255) ) as unit_of_measure  ,

    cast(price_unit as number(38,10) ) as price_unit  ,

    cast(substring(currency_code,1,255) as text(255) ) as currency_code  ,

    cast(deleted_flag as number(38,0) ) as deleted_flag  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,

    cast(source_updated_date as date) as source_updated_date  ,

    cast(source_updated_time as timestamp_ntz(9) ) as source_updated_time  ,

    cast(released_quantity as number(38,10) ) as released_quantity  ,

    cast(received_quantity as number(38,10) ) as received_quantity  ,

    cast(invoiced_quantity as number(38,10) ) as invoiced_quantity  ,

    cast(remain_quantity as number(38,10) ) as remain_quantity,  
 
    cast(unique_key as text(255) ) as unique_key
    from int_fact
    where rownum = 1