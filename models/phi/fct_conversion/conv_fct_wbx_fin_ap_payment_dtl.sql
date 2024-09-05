{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_finance"]
    )
}}

with
old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_fin_ap_payment_dtl") }}
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
    select distinct
        source_object_id,
        source_object_id_old,
        source_company_code,
        account_guid,
        account_guid_old,
        source_account_identifier
    from {{ ref('conv_dim_wbx_account') }}
),

converted_fct as (
    select
        source_system,
        payment_identifier,
        line_number,
        document_company,
        document_type,
        document_number,
        document_pay_item,
        document_pay_item_ext,
        nvl(plnt.source_business_unit_code_new, '-')
            as source_business_unit_code,
        a.source_business_unit_code as source_business_unit_code_old,
        nvl(plnt.plantdc_address_guid_new, '0') as business_unit_address_guid,
        a.business_unit_address_guid as business_unit_address_guid_old,
        source_payee_identifier,
        payee_address_number_guid,
        acnt.source_account_identifier as source_account_identifier,
        a.source_account_identifier as source_account_identifier_old,
        target_account_identifier,
        acnt.account_guid,
        a.account_guid as account_guid_old,
        source_payment_instr_code,
        target_payment_instr_code,
        payment_instr_desc,
        gl_posted_flag,
        purchase_order_number,
        remark_txt,
        transaction_currency,
        base_currency,
        pcomp_currency,
        phi_currency,
        txn_conv_rt,
        base_conv_rt,
        phi_conv_rt,
        pcomp_conv_rt,
        txn_payment_amt,
        txn_discount_available_amt,
        txn_discount_taken_amt,
        base_payment_amt,
        base_discount_available_amt,
        base_discount_taken_amt,
        pcomp_payment_amt,
        pcomp_discount_available_amt,
        pcomp_discount_taken_amt,
        phi_payment_amt,
        phi_discount_available_amt,
        phi_discount_taken_amt,
        payment_trans_doc_number,
        payment_trans_date,
        void_date,
        payment_trans_cleared_date,
        void_flag,
        source_date_updated,
        load_date,
        update_date,
        source_updated_d_id,
        ap_voucher_hdr_unique_key,
        unique_key
    from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid
    left join old_account as acnt on a.account_guid = acnt.account_guid_old
)

select
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(substring(payment_identifier, 1, 255) as text(255))
        as payment_identifier,
    cast(line_number as number(38, 10)) as line_number,
    cast(substring(document_company, 1, 255) as text(255)) as document_company,
    cast(substring(document_type, 1, 255) as text(255)) as document_type,
    cast(substring(document_number, 1, 255) as text(255)) as document_number,
    cast(substring(document_pay_item, 1, 255) as text(255))
        as document_pay_item,
    cast(substring(document_pay_item_ext, 1, 6) as text(6))
        as document_pay_item_ext,
    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,
    cast(
        substring(source_business_unit_code_old, 1, 255) as text(255)
    ) as source_business_unit_code_old,
    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,
    cast(business_unit_address_guid_old as text(255))
        as business_unit_address_guid_old,
    cast(
        substring(source_payee_identifier, 1, 255) as text(255)
    ) as source_payee_identifier,
    cast(payee_address_number_guid as text(255)) as payee_address_number_guid,
    cast(
        substring(source_account_identifier, 1, 255) as text(255)
    ) as source_account_identifier,
    cast(
        substring(source_account_identifier_old, 1, 255) as text(255)
    ) as source_account_identifier_old,
    cast(
        substring(target_account_identifier, 1, 255) as text(255)
    ) as target_account_identifier,
    cast(account_guid as text(255)) as account_guid,
    cast(account_guid_old as text(255)) as account_guid_old,
    cast(
        substring(source_payment_instr_code, 1, 255) as text(255)
    ) as source_payment_instr_code,
    cast(
        substring(target_payment_instr_code, 1, 255) as text(255)
    ) as target_payment_instr_code,
    cast(substring(payment_instr_desc, 1, 255) as text(255))
        as payment_instr_desc,
    cast(substring(gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,
    cast(
        substring(purchase_order_number, 1, 255) as text(255)
    ) as purchase_order_number,
    cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,
    cast(substring(transaction_currency, 1, 255) as text(255))
        as transaction_currency,
    cast(substring(base_currency, 1, 255) as text(255)) as base_currency,
    cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,
    cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,
    cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,
    cast(base_conv_rt as number(29, 9)) as base_conv_rt,
    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
    cast(txn_payment_amt as number(38, 10)) as txn_payment_amt,
    cast(txn_discount_available_amt as number(38, 10))
        as txn_discount_available_amt,
    cast(txn_discount_taken_amt as number(38, 10)) as txn_discount_taken_amt,
    cast(base_payment_amt as number(38, 10)) as base_payment_amt,
    cast(base_discount_available_amt as number(38, 10))
        as base_discount_available_amt,
    cast(base_discount_taken_amt as number(38, 10)) as base_discount_taken_amt,
    cast(pcomp_payment_amt as number(38, 10))
        as pcomp_payment_amt,
    cast(
        pcomp_discount_available_amt as number(38, 10)
    ) as pcomp_discount_available_amt,
    cast(pcomp_discount_taken_amt as number(38, 10))
        as pcomp_discount_taken_amt,
    cast(phi_payment_amt as number(38, 10)) as phi_payment_amt,
    cast(
        phi_discount_available_amt as number(38, 10)
    ) as phi_discount_available_amt,
    cast(
        phi_discount_taken_amt as number(38, 10)
    ) as phi_discount_taken_amt,
    cast(substring(payment_trans_doc_number, 1, 255) as text(255))
        as payment_trans_doc_number,
    cast(payment_trans_date as timestamp_ntz(9)) as payment_trans_date,
    cast(void_date as timestamp_ntz(9)) as void_date,
    cast(payment_trans_cleared_date as timestamp_ntz(9))
        as payment_trans_cleared_date,
    cast(substring(void_flag, 1, 4) as text(4)) as void_flag,
    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(ap_voucher_hdr_unique_key as text(255)) as ap_voucher_hdr_unique_key,
    cast(unique_key as text(255)) as unique_key
from converted_fct
