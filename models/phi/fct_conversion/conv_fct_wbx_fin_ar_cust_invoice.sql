{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_finance"]
    )
}}

/* For AR Customber Invoice conversion of AX History, the following dimension fields are possible for "conversion".
    -Cost Center (aka Plant Dim): source_business_unit_code and plantdc_address_guid
    -Account Dim: source_object_id, account_guid, source_account_identifier
    -Payment Terms: source_payment_terms_code, payment_terms_guid

*/

with
old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_fin_ar_cust_invoice") }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
),

old_plant as (
    select
        source_business_unit_code_new,
        source_business_unit_code,
        plantdc_address_guid_new,
        plantdc_address_guid
    from {{ ref('conv_dim_wbx_plant_dc') }}
),

old_account as (
    select
        source_object_id,
        source_object_id_old,
        source_company_code,
        account_guid,
        account_guid_old,
        source_account_identifier,
        source_account_identifier_old
    from {{ ref('conv_dim_wbx_account') }}
),

old_payment as (
    select
        source_payment_terms_code_new,
        source_payment_terms_code_old,
        payment_terms_guid,
        payment_terms_guid_old
    from {{ ref('conv_xref_wbx_payment_terms') }}
),

converted_fct as (
    select
        cast(substring(acnt.source_object_id, 1, 255) as text(255))
            as source_object_id,
        cast(substring(acnt.source_object_id_old, 1, 255) as text(255))
            as source_object_id_old,
        cast(substring(a.source_system, 1, 255) as text(255)) as source_system,
        cast(substring(a.document_number, 1, 255) as text(255))
            as document_number,
        cast(substring(a.source_document_type, 1, 255) as text(255))
            as source_document_type,
        cast(substring(a.document_type, 1, 255) as text(255)) as document_type,
        cast(substring(a.document_company, 1, 255) as text(255))
            as document_company,
        cast(substring(a.document_pay_item, 1, 255) as text(255))
            as document_pay_item,
        cast(substring(a.source_customer_identifier, 1, 255) as text(255))
            as source_customer_identifier,
        cast(substring(a.customer_address_number_guid, 1, 255) as text(255))
            as customer_address_number_guid,
        cast(a.gl_date as timestamp_ntz(9)) as gl_date,
        cast(a.invoice_date as timestamp_ntz(9)) as invoice_date,
        cast(substring(a.company_code, 1, 255) as text(255)) as company_code,
        cast(substring(a.gl_offset_srccd, 1, 255) as text(255))
            as gl_offset_srccd,
        cast(substring(a.target_account_identifier, 1, 255) as text(255))
            as target_account_identifier,
        cast(substring(a.source_payor_identifier, 1, 255) as text(255))
            as source_payor_identifier,
        cast(substring(a.payor_address_number_guid, 1, 255) as text(255))
            as payor_address_number_guid,
        cast(substring(gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,
        cast(substring(a.pay_status_code, 1, 20) as text(20))
            as pay_status_code,
        cast(substring(foreign_transaction_flag, 1, 20) as text(20))
            as foreign_transaction_flag,
        cast(service_date as timestamp_ntz(9)) as service_date,
        cast(substring(plnt.source_business_unit_code_new, 1, 255) as text(255))
            as source_business_unit_code,
        cast(substring(plnt.source_business_unit_code, 1, 255) as text(255))
            as source_business_unit_code_old,
        cast(substring(plnt.plantdc_address_guid_new, 1, 255) as text(255))
            as business_unit_address_guid,
        cast(substring(plnt.plantdc_address_guid, 1, 255) as text(255))
            as business_unit_address_guid_old,
        cast(
            nvl(
                pymt.source_payment_terms_code_new, a.source_payment_terms_code
            ) as text(255)
        ) as source_payment_terms_code,
        cast(
            nvl(
                pymt.source_payment_terms_code_old, a.source_payment_terms_code
            ) as text(255)
        ) as source_payment_terms_code_old,
        cast(nvl(pymt.payment_terms_guid, a.payment_terms_guid) as text(255))
            as payment_terms_guid,
        cast(
            nvl(pymt.payment_terms_guid_old, a.payment_terms_guid) as text(255)
        ) as payment_terms_guid_old,
        cast(substring(acnt.source_company_code, 1, 255) as text(255))
            as source_company_code,
        cast(substring(acnt.account_guid, 1, 255) as text(255)) as account_guid,
        cast(substring(acnt.account_guid_old, 1, 255) as text(255))
            as account_guid_old,
        cast(substring(acnt.source_account_identifier, 1, 255) as text(255))
            as source_account_identifier,
        cast(substring(acnt.source_account_identifier_old, 1, 255) as text(255))
            as source_account_identifier_old,
        cast(net_due_date as timestamp_ntz(9)) as net_due_date,
        cast(discount_due_date as timestamp_ntz(9)) as discount_due_date,
        cast(projected_payment_date as timestamp_ntz(9))
            as projected_payment_date,
        cast(substring(original_document_number, 1, 255) as text(255))
            as original_document_number,
        cast(substring(original_document_type, 1, 255) as text(255))
            as original_document_type,
        cast(substring(original_document_company, 1, 255) as text(255))
            as original_document_company,
        cast(substring(original_document_pay_item, 1, 255) as text(255))
            as original_document_pay_item,
        cast(substring(supplier_invoice_number, 1, 255) as text(255))
            as supplier_invoice_number,
        cast(substring(sales_document_number, 1, 255) as text(255))
            as sales_document_number,
        cast(substring(sales_document_type, 1, 255) as text(255))
            as sales_document_type,
        cast(substring(sales_document_company, 1, 255) as text(255))
            as sales_document_company,
        cast(substring(sales_document_suffix, 1, 255) as text(255))
            as sales_document_suffix,
        cast(cleared_date as timestamp_ntz(9)) as cleared_date,
        cast(substring(reference_txt, 1, 255) as text(255)) as reference_txt,
        cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,
        cast(substring(name_txt, 1, 255) as text(255)) as name_txt,
        cast(substring(source_item_identifier, 1, 255) as text(255))
            as source_item_identifier,
        cast(substring(item_guid, 1, 255) as text(255)) as item_guid,
        cast(quantity as number(38, 10)) as quantity,
        cast(substring(transaction_uom, 1, 255) as text(255))
            as transaction_uom,
        cast(substring(source_payment_instr_code, 1, 255) as text(255))
            as source_payment_instr_code,
        cast(substring(target_payment_instr_code, 1, 255) as text(255))
            as target_payment_instr_code,
        cast(substring(payment_instr_desc, 1, 255) as text(255))
            as payment_instr_desc,
        cast(void_date as timestamp_ntz(9)) as void_date,
        cast(substring(void_flag, 1, 255) as text(255)) as void_flag,
        cast(substring(payment_identifier, 1, 255) as text(255))
            as payment_identifier,
        cast(invoice_closed_date as timestamp_ntz(9)) as invoice_closed_date,
        cast(substring(deduction_reason_code, 1, 255) as text(255))
            as deduction_reason_code,
        cast(substring(transaction_currency, 1, 255) as text(255))
            as transaction_currency,
        cast(substring(base_currency, 1, 255) as text(255)) as base_currency,
        cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,
        cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,
        cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,
        cast(base_conv_rt as number(29, 9)) as base_conv_rt,
        cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
        cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
        cast(txn_gross_amt as number(38, 10)) as txn_gross_amt,
        cast(txn_open_amt as number(38, 10)) as txn_open_amt,
        cast(txn_discount_available_amt as number(38, 10))
            as txn_discount_available_amt,
        cast(txn_discount_taken_amt as number(38, 10))
            as txn_discount_taken_amt,
        cast(txn_taxable_amt as number(38, 10)) as txn_taxable_amt,
        cast(txn_nontaxable_amt as number(38, 10)) as txn_nontaxable_amt,
        cast(txn_tax_amt as number(38, 10)) as txn_tax_amt,
        cast(base_gross_amt as number(38, 10)) as base_gross_amt,
        cast(base_open_amt as number(38, 10)) as base_open_amt,
        cast(base_discount_available_amt as number(38, 10))
            as base_discount_available_amt,
        cast(base_discount_taken_amt as number(38, 10))
            as base_discount_taken_amt,
        cast(base_taxable_amt as number(38, 10)) as base_taxable_amt,
        cast(base_nontaxable_amt as number(38, 10)) as base_nontaxable_amt,
        cast(base_tax_amt as number(38, 10)) as base_tax_amt,
        cast(phi_gross_amt as number(38, 10)) as phi_gross_amt,
        cast(phi_open_amt as number(38, 10)) as phi_open_amt,
        cast(phi_discount_available_amt as number(38, 10))
            as phi_discount_available_amt,
        cast(phi_discount_taken_amt as number(38, 10))
            as phi_discount_taken_amt,
        cast(phi_taxable_amt as number(38, 10)) as phi_taxable_amt,
        cast(phi_nontaxable_amt as number(38, 10)) as phi_nontaxable_amt,
        cast(phi_tax_amt as number(38, 10)) as phi_tax_amt,
        cast(pcomp_gross_amt as number(38, 10)) as pcomp_gross_amt,
        cast(pcomp_open_amt as number(38, 10)) as pcomp_open_amt,
        cast(pcomp_discount_available_amt as number(38, 10))
            as pcomp_discount_available_amt,
        cast(pcomp_discount_taken_amt as number(38, 10))
            as pcomp_discount_taken_amt,
        cast(pcomp_taxable_amt as number(38, 10)) as pcomp_taxable_amt,
        cast(pcomp_nontaxable_amt as number(38, 10)) as pcomp_nontaxable_amt,
        cast(pcomp_tax_amt as number(38, 10)) as pcomp_tax_amt,
        cast(substring(intercompany_flag, 1, 1) as text(1))
            as intercompany_flag,
        cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
        cast(load_date as timestamp_ntz(9)) as load_date,
        cast(update_date as timestamp_ntz(9)) as update_date,
        cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
        cast(substring(batch_type, 1, 4) as text(4)) as batch_type,
        cast(substring(batch_number, 1, 255) as text(255)) as batch_number,
        cast(batch_date as timestamp_ntz(9)) as batch_date,
        cast(substring(ar_custinv_hdr_unique_key, 1, 255) as text(255))
            as ar_custinv_hdr_unique_key,
        cast(substring(unique_key, 1, 255) as text(255)) as unique_key
    from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid
    left join
        old_payment as pymt
        on a.payment_terms_guid = pymt.payment_terms_guid_old
    left join old_account as acnt on a.account_guid = acnt.account_guid_old
)

select *
from converted_fct

