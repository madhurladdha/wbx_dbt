with
    source as (select * from {{ source("R_EI_SYSADM", "prc_wtx_agrmnt_snapshot_fact") }} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y') ,

    renamed as (

        select
           

    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(snapshot_date as date) as snapshot_date  ,

    cast(substring(agreement_number,1,255) as text(255) ) as agreement_number  ,

    cast(line_number as number(38,10) ) as line_number  ,

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
                {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "source_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }} as business_unit_address_guid,
            {{
                dbt_utils.surrogate_key(
                    ["source_system", "source_item_identifier"]
                )
            }} as item_guid,

    

    --cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,
    --cast(item_guid as text(255) ) as item_guid  ,
    cast(po_order_count as number(38,0) ) as po_order_count  ,

    cast(po_ordered_qty as number(38,0) ) as po_ordered_qty  ,

    cast(receipt_received_qty as number(38,0) ) as receipt_received_qty ,

    cast(source_updated_date as date) as source_updated_date  ,

    cast(source_updated_time as timestamp_ntz(6) ) as source_updated_time  ,

    cast(released_quantity as number(38,10) ) as released_quantity  ,

    cast(received_quantity as number(38,10) ) as received_quantity  ,

    cast(invoiced_quantity as number(38,10) ) as invoiced_quantity  ,

    cast(remain_quantity as number(38,10) ) as remain_quantity  
 
  --  cast(unique_key as text(255) ) as unique_key 

        from source

    )

select 
cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(snapshot_date as date) as snapshot_date  ,

    cast(substring(agreement_number,1,255) as text(255) ) as agreement_number  ,

    cast(line_number as number(38,10) ) as line_number  ,

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
    cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,
    cast(item_guid as text(255) ) as item_guid  ,
    cast(po_order_count as number(38,0) ) as po_order_count  ,

    cast(po_ordered_qty as number(38,0) ) as po_ordered_qty  ,

    cast(receipt_received_qty as number(38,0) ) as receipt_received_qty ,

    cast(source_updated_date as date) as source_updated_date  ,

    cast(source_updated_time as timestamp_ntz(6) ) as source_updated_time  ,

    cast(released_quantity as number(38,10) ) as released_quantity  ,

    cast(received_quantity as number(38,10) ) as received_quantity  ,

    cast(invoiced_quantity as number(38,10) ) as invoiced_quantity  ,

    cast(remain_quantity as number(38,10) ) as remain_quantity  
 
  --  cast(unique_key as text(255) ) as unique_key 
from renamed
