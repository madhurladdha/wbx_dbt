{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_finance"]
    )
}}

/* For AR Customer Invoice Header, the following dimension fields are possible for "conversion".
    -Plant Dim: source_business_unit_code and plantdc_address_guid
    -Account Dim: source_object_id, account_guid, source_account_identifier
    -Payment Terms: source_payment_terms_code, payment_terms_guid
*/

with
old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_fin_ar_custinv_hdr") }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
), --make sure this flag is set to yes, this allows the system to pull the history. Without this, history will not come through.

--cross references from history that we will need to convert from old to new
/* For AR Customer Invoice Header, it appears as though the source_business_unit_code (plant) is always NULL.
    This means that we can do this "conversion" of plant, but there is not impact since all are NULL.
*/
old_plant as (
    select
        source_business_unit_code_new,
        source_business_unit_code,
        plantdc_address_guid_new,
        plantdc_address_guid
    from {{ ref('conv_dim_wbx_plant_dc') }}
),

--use this format for any account sources matching the reference below (this should be the full list of columns needed)
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
        cast(substring(a.source_system, 1, 255) as text(255)) as source_system,
        cast(substring(a.document_number, 1, 255) as text(255))
            as document_number,
        cast(substring(a.source_document_type, 1, 255) as text(255))
            as source_document_type,
        cast(substring(a.document_type, 1, 255) as text(255)) as document_type,
        cast(substring(a.document_company, 1, 255) as text(255))
            as document_company,
        cast(substring(a.source_customer_identifier, 1, 255) as text(255))
            as source_customer_identifier,
        cast(substring(a.customer_address_number_guid, 1, 255) as text(255))
            as customer_address_number_guid,
        cast(a.gl_date as timestamp_ntz(9)) as gl_date,
        cast(a.invoice_date as timestamp_ntz(9)) as invoice_date,
        cast(substring(a.company_code, 1, 255) as text(255)) as company_code,
        cast(substring(a.target_account_identifier, 1, 255) as text(255))
            as target_account_identifier,
        cast(substring(a.gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,
        cast(substring(a.pay_status_code, 1, 20) as text(20))
            as pay_status_code,
        cast(substring(a.foreign_transaction_flag, 1, 20) as text(20))
            as foreign_transaction_flag,
        cast(a.service_date as timestamp_ntz(9)) as service_date,
        cast(substring(plnt.source_business_unit_code_new, 1, 255) as text(255))
            as source_business_unit_code,
        cast(substring(plnt.source_business_unit_code, 1, 255) as text(255))
            as source_business_unit_code_old,
        cast(substring(plnt.plantdc_address_guid_new, 1, 255) as text(255))
            as business_unit_address_guid,
        cast(substring(plnt.plantdc_address_guid, 1, 255) as text(255))
            as business_unit_address_guid_old,
        cast(substring(acnt.source_object_id, 1, 255) as text(255))
            as source_object_id,
        cast(substring(acnt.source_object_id_old, 1, 255) as text(255))
            as source_object_id_old,
        cast(
            substring(pymt.source_payment_terms_code_new, 1, 255) as text(255)
        ) as source_payment_terms_code,
        cast(
            substring(pymt.source_payment_terms_code_old, 1, 255) as text(255)
        ) as source_payment_terms_code_old,
        cast(substring(pymt.payment_terms_guid, 1, 255) as text(255))
            as payment_terms_guid,
        cast(substring(pymt.payment_terms_guid_old, 1, 255) as text(255))
            as payment_terms_guid_old,
        cast(substring(acnt.source_company_code, 1, 255) as text(255))
            as source_company_code,
        cast(substring(acnt.account_guid, 1, 255) as text(255)) as account_guid,
        cast(substring(acnt.account_guid_old, 1, 255) as text(255))
            as account_guid_old,
        cast(substring(acnt.source_account_identifier, 1, 255) as text(255))
            as source_account_identifier,
        cast(
            substring(acnt.source_account_identifier_old, 1, 255) as text(255)
        ) as source_account_identifier_old,
        cast(net_due_date as timestamp_ntz(9)) as net_due_date,
        cast(discount_due_date as timestamp_ntz(9)) as discount_due_date,
        cast(projected_payment_date as timestamp_ntz(9))
            as projected_payment_date,
        cast(substring(a.sales_document_number, 1, 255) as text(255))
            as sales_document_number,
        cast(substring(a.sales_document_type, 1, 255) as text(255))
            as sales_document_type,
        cast(substring(a.sales_document_company, 1, 255) as text(255))
            as sales_document_company,
        cast(cleared_date as timestamp_ntz(9)) as cleared_date,
        cast(substring(a.reference_txt, 1, 255) as text(255)) as reference_txt,
        cast(substring(a.name_txt, 1, 255) as text(255)) as name_txt,
        cast(substring(a.source_payment_instr_code, 1, 255) as text(255))
            as source_payment_instr_code,
        cast(substring(a.target_payment_instr_code, 1, 255) as text(255))
            as target_payment_instr_code,
        cast(substring(a.payment_instr_desc, 1, 255) as text(255))
            as payment_instr_desc,
        cast(substring(a.payment_identifier, 1, 255) as text(255))
            as payment_identifier,
        cast(a.invoice_closed_date as timestamp_ntz(9)) as invoice_closed_date,
        cast(substring(a.transaction_currency, 1, 255) as text(255))
            as transaction_currency,
        cast(substring(a.base_currency, 1, 255) as text(255)) as base_currency,
        cast(substring(a.phi_currency, 1, 255) as text(255)) as phi_currency,
        cast(substring(a.pcomp_currency, 1, 255) as text(255))
            as pcomp_currency,
        cast(a.txn_conv_rt as number(29, 9)) as txn_conv_rt,
        cast(a.base_conv_rt as number(29, 9)) as base_conv_rt,
        cast(a.phi_conv_rt as number(29, 9)) as phi_conv_rt,
        cast(a.pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
        cast(a.txn_gross_amt as number(38, 10)) as txn_gross_amt,
        cast(a.txn_open_amt as number(38, 10)) as txn_open_amt,
        cast(a.txn_discount_available_amt as number(38, 10))
            as txn_discount_available_amt,
        cast(a.txn_discount_taken_amt as number(38, 10))
            as txn_discount_taken_amt,
        cast(a.txn_taxable_amt as number(38, 10)) as txn_taxable_amt,
        cast(a.txn_nontaxable_amt as number(38, 10)) as txn_nontaxable_amt,
        cast(a.txn_tax_amt as number(38, 10)) as txn_tax_amt,
        cast(a.txn_purchase_charge_amt as number(38, 10))
            as txn_purchase_charge_amt,
        cast(a.base_gross_amt as number(38, 10)) as base_gross_amt,
        cast(a.base_open_amt as number(38, 10)) as base_open_amt,
        cast(a.base_discount_available_amt as number(38, 10))
            as base_discount_available_amt,
        cast(a.base_discount_taken_amt as number(38, 10))
            as base_discount_taken_amt,
        cast(a.base_taxable_amt as number(38, 10)) as base_taxable_amt,
        cast(a.base_nontaxable_amt as number(38, 10)) as base_nontaxable_amt,
        cast(base_tax_amt as number(38, 10)) as base_tax_amt,
        cast(a.base_purchase_charge_amt as number(38, 10))
            as base_purchase_charge_amt,
        cast(a.phi_gross_amt as number(38, 10)) as phi_gross_amt,
        cast(a.phi_open_amt as number(38, 10)) as phi_open_amt,
        cast(a.phi_discount_available_amt as number(38, 10))
            as phi_discount_available_amt,
        cast(a.phi_discount_taken_amt as number(38, 10))
            as phi_discount_taken_amt,
        cast(a.phi_taxable_amt as number(38, 10)) as phi_taxable_amt,
        cast(a.phi_nontaxable_amt as number(38, 10)) as phi_nontaxable_amt,
        cast(a.phi_tax_amt as number(38, 10)) as phi_tax_amt,
        cast(a.phi_purchase_charge_amt as number(38, 10))
            as phi_purchase_charge_amt,
        cast(a.pcomp_gross_amt as number(38, 10)) as pcomp_gross_amt,
        cast(a.pcomp_open_amt as number(38, 10)) as pcomp_open_amt,
        cast(a.pcomp_discount_available_amt as number(38, 10))
            as pcomp_discount_available_amt,
        cast(a.pcomp_discount_taken_amt as number(38, 10))
            as pcomp_discount_taken_amt,
        cast(a.pcomp_taxable_amt as number(38, 10)) as pcomp_taxable_amt,
        cast(a.pcomp_nontaxable_amt as number(38, 10)) as pcomp_nontaxable_amt,
        cast(a.pcomp_tax_amt as number(38, 10)) as pcomp_tax_amt,
        cast(a.pcomp_purchase_charge_amt as number(38, 10))
            as pcomp_purchase_charge_amt,
        cast(substring(a.intercompany_flag, 1, 1) as text(1))
            as intercompany_flag,
        cast(a.source_date_updated as timestamp_ntz(9)) as source_date_updated,
        cast(a.load_date as timestamp_ntz(9)) as load_date,
        cast(a.update_date as timestamp_ntz(9)) as update_date,
        cast(a.source_updated_d_id as number(38, 0)) as source_updated_d_id,
        cast(a.unique_key as varchar(255)) as unique_key
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