{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_finance"]
    )
}}

with old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_fin_ap_voucher") }}
    where
        {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
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
        source_account_identifier
    from {{ ref('conv_dim_wbx_account') }}
),

old_payment as (
    select
        source_payment_terms_code_new,
        payment_terms_guid,
        payment_terms_guid_old
    from {{ ref('conv_xref_wbx_payment_terms') }}
),

converted_fct as (
    select
        source_system,
        document_company,
        document_number,
        document_type,
        document_pay_item,
        document_pay_item_ext,
        plnt.source_business_unit_code_new as source_business_unit_code,
        plnt.plantdc_address_guid_new as business_unit_address_guid,
        a.source_business_unit_code as source_business_unit_code_old,
        a.business_unit_address_guid as business_unit_address_guid_old,
        source_supplier_identifier,
         {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "source_supplier_identifier",
                        "'SUPPLIER'",
                        "document_company",
                    ]
                )
            }} as supplier_address_number_guid,
        pymt.source_payment_terms_code_new as source_payment_terms_code,
        pymt.payment_terms_guid as payment_terms_guid,
        acnt.source_account_identifier as source_account_identifier,
        a.source_payment_terms_code as source_payment_terms_code_old,
        a.payment_terms_guid as payment_terms_guid_old,
        a.source_account_identifier as source_account_identifier_old,
        target_account_identifier,
        acnt.account_guid,
        a.account_guid as account_guid_old,
        source_payee_identifier,
        payee_address_number_guid,
        source_item_identifier,
        item_guid,
        source_payment_instr_code,
        target_payment_instr_code,
        payment_instr_desc,
        invoice_date,
        voucher_date,
        net_due_date,
        discount_date,
        gl_date,
        company_code,
        pay_status_code,
        transaction_currency,
        base_currency,
        pcomp_currency,
        phi_currency,
        txn_conv_rt,
        base_conv_rt,
        phi_conv_rt,
        pcomp_conv_rt,
        txn_gross_amt,
        txn_open_amt,
        txn_discount_available_amt,
        txn_discount_taken_amt,
        txn_taxable_amt,
        txn_tax_amt,
        base_gross_amt,
        base_open_amt,
        base_discount_available_amt,
        base_discount_taken_amt,
        base_taxable_amt,
        base_tax_amt,
        pcomp_gross_amt,
        pcomp_open_amt,
        pcomp_discount_available_amt,
        pcomp_discount_taken_amt,
        pcomp_taxable_amt,
        pcomp_tax_amt,
        phi_gross_amt,
        phi_open_amt,
        phi_discount_available_amt,
        phi_discount_taken_amt,
        phi_taxable_amt,
        phi_tax_amt,
        gl_offset_srccd,
        gl_posted_flag,
        void_flag,
        supplier_invoice_number,
        reference_txt,
        remark_txt,
        quantity,
        transaction_uom,
        source_date_updated,
        load_date,
        update_date,
        source_updated_d_id,
        ap_voucher_hdr_unique_key,
        unique_key,
        po_order_number,
        approver,
        source_document_type
    from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid
    left join
        old_payment as pymt
        on a.payment_terms_guid = pymt.payment_terms_guid_old
    left join old_account as acnt on a.account_guid = acnt.account_guid_old
)

select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(substring(document_company, 1, 255) as text(255)) as document_company,
    cast(substring(document_number, 1, 255) as text(255)) as document_number,
    cast(substring(document_type, 1, 255) as text(255)) as document_type,
    cast(substring(document_pay_item, 1, 255) as text(255))
        as document_pay_item,
    cast(substring(document_pay_item_ext, 1, 255) as text(255))
        as document_pay_item_ext,
    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,
    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,
    cast(
        substring(source_business_unit_code_old, 1, 255) as text(255)
    ) as source_business_unit_code_old,
    cast(business_unit_address_guid_old as text(255))
        as business_unit_address_guid_old,
    cast(
        substring(source_supplier_identifier, 1, 255) as text(255)
    ) as source_supplier_identifier,
    cast(supplier_address_number_guid as text(255))
        as supplier_address_number_guid,
    cast(
        substring(source_payment_terms_code, 1, 255) as text(255)
    ) as source_payment_terms_code,
    cast(payment_terms_guid as text(255)) as payment_terms_guid,
    cast(
        substring(source_account_identifier, 1, 255) as text(255)
    ) as source_account_identifier,
    cast(
        substring(source_payment_terms_code_old, 1, 255) as text(255)
    ) as source_payment_terms_code_old,
    cast(payment_terms_guid_old as text(255)) as payment_terms_guid_old,
    cast(
        substring(source_account_identifier_old, 1, 255) as text(255)
    ) as source_account_identifier_old,
    cast(
        substring(target_account_identifier, 1, 255) as text(255)
    ) as target_account_identifier,
    cast(account_guid as text(255)) as account_guid,
    cast(account_guid_old as text(255)) as account_guid_old,
    cast(
        substring(source_payee_identifier, 1, 255) as text(255)
    ) as source_payee_identifier,
    cast(payee_address_number_guid as text(255)) as payee_address_number_guid,
    cast(
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,
    cast(item_guid as text(255)) as item_guid,
    cast(
        substring(source_payment_instr_code, 1, 255) as text(255)
    ) as source_payment_instr_code,
    cast(
        substring(target_payment_instr_code, 1, 255) as text(255)
    ) as target_payment_instr_code,
    cast(substring(payment_instr_desc, 1, 255) as text(255))
        as payment_instr_desc,
    cast(invoice_date as timestamp_ntz(9)) as invoice_date,
    cast(voucher_date as timestamp_ntz(9)) as voucher_date,
    cast(net_due_date as timestamp_ntz(9)) as net_due_date,
    cast(discount_date as timestamp_ntz(9)) as discount_date,
    cast(gl_date as timestamp_ntz(9)) as gl_date,
    cast(substring(company_code, 1, 255) as text(255)) as company_code,
    cast(substring(pay_status_code, 1, 20) as text(20)) as pay_status_code,
    cast(substring(transaction_currency, 1, 255) as text(255))
        as transaction_currency,
    cast(substring(base_currency, 1, 255) as text(255)) as base_currency,
    cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,
    cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,
    cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,
    cast(base_conv_rt as number(29, 9)) as base_conv_rt,
    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
    cast(txn_gross_amt as number(38, 10)) as txn_gross_amt,
    cast(txn_open_amt as number(38, 10)) as txn_open_amt,
    cast(txn_discount_available_amt as number(38, 10))
        as txn_discount_available_amt,
    cast(txn_discount_taken_amt as number(38, 10)) as txn_discount_taken_amt,
    cast(txn_taxable_amt as number(38, 10)) as txn_taxable_amt,
    cast(txn_tax_amt as number(38, 10)) as txn_tax_amt,
    cast(base_gross_amt as number(38, 10)) as base_gross_amt,
    cast(base_open_amt as number(38, 10)) as base_open_amt,
    cast(base_discount_available_amt as number(38, 10))
        as base_discount_available_amt,
    cast(base_discount_taken_amt as number(38, 10)) as base_discount_taken_amt,
    cast(base_taxable_amt as number(38, 10)) as base_taxable_amt,
    cast(base_tax_amt as number(38, 10)) as base_tax_amt,
    cast(
        pcomp_gross_amt as number(38, 10)
    ) as pcomp_gross_amt,
    cast(
        pcomp_open_amt as number(38, 10)
    ) as pcomp_open_amt,
    cast(
        pcomp_discount_available_amt as number(38, 10)
    ) as pcomp_discount_available_amt,
    cast(
        pcomp_discount_taken_amt as number(38, 10)
    ) as pcomp_discount_taken_amt,
    cast(
        pcomp_taxable_amt as number(38, 10)
    ) as pcomp_taxable_amt,
    cast(pcomp_tax_amt as number(38, 10)) as pcomp_tax_amt,
    cast(phi_gross_amt as number(38, 10)) as phi_gross_amt,
    cast(phi_open_amt as number(38, 10)) as phi_open_amt,
    cast(
        phi_discount_available_amt as number(38, 10)
    ) as phi_discount_available_amt,
    cast(
        phi_discount_taken_amt as number(38, 10)
    ) as phi_discount_taken_amt,
    cast(
        phi_taxable_amt as number(38, 10)
    ) as phi_taxable_amt,
    cast(phi_tax_amt as number(38, 10)) as phi_tax_amt,
    cast(substring(gl_offset_srccd, 1, 255) as text(255)) as gl_offset_srccd,
    cast(substring(gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,
    cast(substring(void_flag, 1, 4) as text(4)) as void_flag,
    cast(
        substring(supplier_invoice_number, 1, 255) as text(255)
    ) as supplier_invoice_number,
    cast(substring(reference_txt, 1, 255) as text(255)) as reference_txt,
    cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,
    cast(quantity as number(38, 10)) as quantity,
    cast(substring(transaction_uom, 1, 255) as text(255)) as transaction_uom,
    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(
        ap_voucher_hdr_unique_key as text(255)
    ) as ap_voucher_hdr_unique_key,
    cast(
        unique_key as text(255)
    ) as unique_key,
    cast(po_order_number as text(255)) as po_order_number,
    cast(substring(approver, 1, 255) as text(255)) as approver,
    cast(substring(source_document_type, 1, 255) as text(255))
        as source_document_type
from converted_fct


