{{ config(
    tags=["wbx", "manufacturing","stock","yield"]
) }}

with stage_table as (
    select * from {{ ref('stg_f_wbx_mfg_inv_stock_trans') }}
),

source as (
    select
        source_system,
        source_transaction_key,
        source_record_id,
        related_document_number,
        source_item_identifier,
        source_business_unit_code,
        variant_code,
        transaction_date,
        gl_date,
        transaction_qty,
        transaction_amt,
        transaction_uom,
        transaction_currency,
        status_code,
        status_desc,
        voucher,
        adjustment_amt,
        load_date,
        update_date,
        company_code,
        site,
        product_class,
        stock_site,
        invoice_returned_flag,
        item_model_group,
        {{ dbt_utils.surrogate_key(['source_system','source_item_identifier']) }} as item_guid,
        {{ dbt_utils.surrogate_key(['source_system','source_business_unit_code',"'PLANT_DC'"]) }} as business_unit_address_guid
    from stage_table
),

gen_unique_key as (
    select
        source.*,
        {{
            dbt_utils.surrogate_key(
                [
                    "cast(substring(source_system,1,255) as text(255) )",
                    "cast(substring(source_transaction_key,1,255) as text(255) )",
                    "cast(source_record_id as number(38,0) )",
                    "cast(substring(related_document_number,1,255) as text(255) )",
                    "cast(substring(source_item_identifier,1,255) as text(255) )",
                    "cast(substring(source_business_unit_code,1,255) as text(255) )",
                    "cast(substring(variant_code,1,255) as text(255) )",
                    "cast(gl_date as timestamp_ntz(9) )"
                ]
            )
        }} as unique_key
    from source
),

final as (
    select 
        cast(substring(source_system,1,255) as text(255) ) as source_system  ,
        cast(substring(source_transaction_key,1,255) as text(255) ) as source_transaction_key  ,
        cast(source_record_id as number(38,0) ) as source_record_id  ,
        cast(substring(related_document_number,1,255) as text(255) ) as related_document_number  ,
        cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
        cast(item_guid as text(255) ) as item_guid  ,
        cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
        cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,
        cast(substring(variant_code,1,255) as text(255) ) as variant_code  ,
        cast(transaction_date as date) as transaction_date  ,
        cast(gl_date as timestamp_ntz(9) ) as gl_date  ,
        cast(transaction_qty as number(20,4) ) as transaction_qty  ,
        cast(transaction_amt as number(20,4) ) as transaction_amt  ,
        cast(substring(transaction_uom,1,255) as text(255) ) as transaction_uom  ,
        cast(substring(transaction_currency,1,255) as text(255) ) as transaction_currency  ,
        cast(substring(status_code,1,255) as text(255) ) as status_code  ,
        cast(substring(status_desc,1,255) as text(255) ) as status_desc  ,
        cast(substring(voucher,1,255) as text(255) ) as voucher  ,
        cast(adjustment_amt as number(20,4) ) as adjustment_amt  ,
        cast(update_date as date) as update_date  ,
        cast(substring(company_code,1,255) as text(255) ) as company_code  ,
        cast(substring(site,1,255) as text(255) ) as site  ,
        cast(substring(product_class,1,255) as text(255) ) as product_class  ,
        cast(load_date as timestamp_ntz(9) ) as load_date  ,
        cast(substring(stock_site,1,255) as text(255) ) as stock_site  ,
        cast(invoice_returned_flag as number(38,0) ) as invoice_returned_flag  ,
        cast(substring(item_model_group,1,255) as text(255) ) as item_model_group  , 
        cast(unique_key as text(255) ) as unique_key
    from gen_unique_key
)

select * from final