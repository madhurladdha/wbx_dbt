{{
    config(
        tags = ["wbx","manufacturing","work order","gl","yield"]
    )
}}
with stage_table as (
    select * from {{ ref('stg_f_wbx_mfg_wo_gl') }}
),

dim_date as (
    select * from {{ref('src_dim_date')}}
),

source as (
    select 
        source_system,
        document_company,
        document_type,
        document_number,
        voucher,
        journal_number,
        reference_id,
        gl_date,
        source_site_code,
        source_business_unit_code,
        cost_center_code,
        source_date_updated,
        cast(substr(stage_table.load_date,1,10) as date )as load_date,
        cast(substr(stage_table.update_date,1,10)  as date ) as update_date,
        product_class,
        source_account_identifier,
        transaction_amount,
        transaction_currency,
        remark_txt,
        recipecalc_date,
        dim_date.calendar_date_id as source_updated_d_id,
        document_company
            || '.'
            || cost_center_code
            || '.'
            || source_account_identifier as source_concat_nat_key,
        {{ dbt_utils.surrogate_key(['source_system','source_concat_nat_key']) }} AS account_guid
    from stage_table
    left join dim_date 
        on dim_date.calendar_date = date(stage_table.source_date_updated)
),

gen_unique_key as (
    select
        source.*,
        {{
            dbt_utils.surrogate_key(
                [
                    "cast(substring(source_system,1,255) as text(255) )",
                    "cast(substring(document_company,1,255) as text(255) )",
                    "cast(substring(document_type,1,255) as text(255) )",
                    "cast(substring(document_number,1,255) as text(255) )",
                ]
            )
        }} as unique_key 
    from source
),

final as (
    select 
        cast(substring(source_system,1,255) as text(255) ) as source_system  ,
        cast(substring(document_company,1,255) as text(255) ) as document_company  ,
        cast(substring(document_type,1,255) as text(255) ) as document_type  ,
        cast(substring(document_number,1,255) as text(255) ) as document_number  ,
        cast(substring(voucher,1,255) as text(255) ) as voucher  ,
        cast(substring(journal_number,1,255) as text(255) ) as journal_number  ,
        cast(substring(reference_id,1,255) as text(255) ) as reference_id  ,
        cast(gl_date as date) as gl_date  ,
        cast(substring(source_site_code,1,255) as text(255) ) as source_site_code  ,
        cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
        cast(substring(cost_center_code,1,255) as text(255) ) as cost_center_code  ,
        cast(source_date_updated as date) as source_date_updated  ,
        cast(load_date as timestamp_ntz(9) ) as load_date  ,
        cast(update_date as timestamp_ntz(9) ) as update_date  ,
        cast(source_updated_d_id as number(38,0) ) as source_updated_d_id  ,
        cast(substring(product_class,1,255) as text(255) ) as product_class  ,
        cast(substring(source_account_identifier,1,255) as text(255) ) as source_account_identifier  ,
        cast(account_guid as text(255) ) as account_guid  ,
        cast(transaction_amount as number(22,7) ) as transaction_amount  ,
        cast(substring(transaction_currency,1,255) as text(255) ) as transaction_currency  ,
        cast(substring(remark_txt,1,255) as text(255) ) as remark_txt  ,
        cast(recipecalc_date as timestamp_ntz(9) ) as recipecalc_date  , 
        cast(unique_key as text(255) ) as unique_key 
    from gen_unique_key
)

select * from final
