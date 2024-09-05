{{ config(tags=["manufacturing","agreement","wbx"]) }}
with
    src_stg as (select * from {{ ref("stg_f_wbx_prc_agreement") }}),
    source as (
        select
            source_system,
            agreement_number,
            line_number,
            agreement_type_desc,
            source_company,
            source_business_unit_code,
            source_item_identifier,
            variant_code,
            site_code,
            status_code,
            status_desc,
            approval_status_code,
            approval_status_desc,
            agreement_eff_date,
            agreement_exp_date,
            supplier_address_number,
            agreement_quantity,
            original_quantity,
            price_per_unit,
            unit_of_measure,
            price_unit,
            currency_code,
            deleted_flag,
            released_quantity,
            received_quantity,
            invoiced_quantity,
            source_update_date,
            source_updated_time
        from src_stg
    ),

    tfm as (
        select
            s.source_system,
            s.agreement_number,
            s.line_number,
            s.agreement_type_desc,
            s.source_company,
            s.source_business_unit_code,
            s.source_item_identifier,
            {{
                dbt_utils.surrogate_key(
                    [
                        "s.source_system",
                        "s.source_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }} as business_unit_address_guid,
            {{
                dbt_utils.surrogate_key(
                    ["s.source_system", "s.source_item_identifier"]
                )
            }} as item_guid,
            s.variant_code,
            s.site_code,
            s.status_code,
            s.status_desc,
            s.approval_status_code,
            s.approval_status_desc,
            s.agreement_eff_date,
            s.agreement_exp_date,
            s.supplier_address_number,
            s.agreement_quantity,
            s.original_quantity,
            s.price_per_unit,
            s.unit_of_measure,
            s.price_unit,
            s.currency_code,
            s.deleted_flag,
            s.released_quantity,
            s.received_quantity,
            s.invoiced_quantity,
            iff(
                (
                    s.agreement_quantity
                    - (s.released_quantity + s.received_quantity + s.invoiced_quantity)
                is null),
                0,
                s.agreement_quantity
                - (s.released_quantity + s.received_quantity + s.invoiced_quantity)
            ) as remain_quantity,
            s.source_update_date as source_updated_date,
            s.source_updated_time as source_updated_time
        from source s
    ),

int_cast as (select 

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

    cast(remain_quantity as number(38,10) ) as remain_quantity  
 
   -- cast(unique_key as text(255) ) as unique_key
from tfm
),

final as (
    select i.*, 
 {{dbt_utils.surrogate_key(
        [
            "i.agreement_number",
            "i.line_number",
            "i.agreement_type_desc",
            "i.source_company"         
        ]
    )
    }} as unique_key
from int_cast i)

select * from final