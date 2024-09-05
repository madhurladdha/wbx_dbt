

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'mfg_wtx_wo_gl_fact') }}

),

renamed as (

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
        load_date,
        update_date,
        source_updated_d_id,
        product_class,
        source_account_identifier,
        account_guid,
        transaction_amount,
        transaction_currency,
        remark_txt,
        recipecalc_date

    from source

)

select * from renamed
