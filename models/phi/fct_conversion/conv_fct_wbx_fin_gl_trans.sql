{{
    config(
    enabled=true,
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_finance"]
    )
}}

/* For GL Trans conversion of AX History, the following dimension fields are possible for "conversion".
    -Cost Center (aka Plant Dim): source_business_unit_code and plantdc_address_guid
    -Account Dim: source_object_id, account_guid, source_account_identifier

    Since the Account Dim is converted and source_account_identifier is part of the unique_key generation, we have to recreate the new unique_key in this code.
    Note that the data set for AX History is quite large at this time so the conversion and later the UNION can take 15-20 minutes using a XS Snowflake WH.

    Specific to GL related tables, a filter is added on DOCUMENT_COMPANY to ensure that we only pull over WBX and RFL.  These are the only companies of interest
    and this simplifies the data set.  The old historical data from AX for other companies would always still remain in WBX_PROD.FACT.FCT_WBX_FIN_GL_GRANS.
*/

with old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_fin_gl_trans") }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
    and document_company in ('WBX','RFL')
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
        account_guid,
        account_guid_old,
        source_object_id,
        source_object_id_old,
        source_account_identifier,
        source_account_identifier_old
    from {{ ref('conv_dim_wbx_account') }}
),

converted_fct as (
    select
        unique_key as unique_key,
        source_system as source_system,
        document_company as document_company,
        source_document_type as source_document_type,
        document_type as document_type,
        document_number as document_number,
        a.source_account_identifier as source_account_identifier_old,
        acnt.source_account_identifier as source_account_identifier,
        gl_date as gl_date,
        journal_line_number as journal_line_number,
        gl_extension_code as gl_extension_code,
        a.source_business_unit_code as source_business_unit_code_old,
        plnt.source_business_unit_code_new as source_business_unit_code,
        void_flag as void_flag,
        payment_trans_date as payment_trans_date,
        document_pay_item as document_pay_item,
        payment_number as payment_number,
        payment_identifier as payment_identifier,
        base_currency as base_currency,
        target_account_identifier as target_account_identifier,
        acnt.account_guid as account_guid,
        source_ledger_type as source_ledger_type,
        ledger_type as ledger_type,
        gl_posted_flag as gl_posted_flag,
        batch_number as batch_number,
        batch_type as batch_type,
        batch_date as batch_date,
        --hardcorded to '-' in upstream models ,so no conversion applied
        source_address_number as source_address_number,
        --hardcorded to '-' in upstream models ,so no conversion applied
        address_guid as address_guid,
        company_code as company_code,
        a.source_object_id as source_object_id_old,
        acnt.source_object_id as source_object_id,
        source_subsidary_id as source_subsidary_id,
        source_company_code as source_company_code,
        a.business_unit_address_guid as business_unit_address_guid_old,
        plnt.plantdc_address_guid_new as business_unit_address_guid,
        txn_ledger_amt as txn_ledger_amt,
        transaction_currency as transaction_currency,
        base_ledger_amt as base_ledger_amt,
        quantity as quantity,
        supplier_invoice_number as supplier_invoice_number,
        invoice_date as invoice_date,
        account_cat21_code as account_cat21_code,
        source_date_updated as source_date_updated,
        load_date as load_date,
        update_date as update_date,
        source_updated_d_id as source_updated_d_id,
        explanation_txt as explanation_txt,
        remark_txt as remark_txt,
        reference1_txt as reference1_txt,
        reference2_txt as reference2_txt,
        reference3_txt as reference3_txt,
        transaction_uom as transaction_uom,
        department as department,
        caf_no as caf_no,
        plant as plant,
        phi_ledger_amt as phi_ledger_amt,
        pcomp_ledger_amt as pcomp_ledger_amt,
        txn_conv_rt as txn_conv_rt,
        phi_currency as phi_currency,
        phi_conv_rt as phi_conv_rt,
        pcomp_currency as pcomp_currency,
        pcomp_conv_rt as pcomp_conv_rt,
        product_dim as product_dim,
        customer_dim as customer_dim,
        oc_txn_ledger_amt as oc_txn_ledger_amt,
        oc_base_ledger_amt as oc_base_ledger_amt,
        oc_pcomp_ledger_amt as oc_pcomp_ledger_amt,
        oc_phi_ledger_amt as oc_phi_ledger_amt,
        journal_category as journal_category,
        'AX' as source_legacy
    from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid
    left join old_account as acnt on a.account_guid = acnt.account_guid_old
),

/* As one of the key values, source_account_identifier, has changed in this conversion we have to regenerate the unique_key based on that.
*/
unique_key_regen as 
(
    select *,
        {{ dbt_utils.surrogate_key(
            [
                "source_system",
                "document_company",
                "source_document_type",
                "document_type",
                "document_number",
                "source_account_identifier",
                "gl_date",
                "journal_line_number",
                "gl_extension_code",
            ]
            )
        }} as unique_key_new
    from converted_fct
)

select
    cast(unique_key_new as text(255)) as unique_key,
    cast(unique_key as text(255)) as unique_key_old,
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(substring(document_company, 1, 20) as text(20)) as document_company,
    cast(substring(source_document_type, 1, 20) as text(20))
        as source_document_type,
    cast(substring(document_type, 1, 20) as text(20)) as document_type,
    cast(substring(document_number, 1, 255) as text(255)) as document_number,
    cast(substring(source_account_identifier_old, 1, 255) as text(255))
        as source_account_identifier_old,
    cast(substring(source_account_identifier, 1, 255) as text(255))
        as source_account_identifier,
    cast(gl_date as timestamp_ntz(9)) as gl_date,
    cast(journal_line_number as number(38, 0)) as journal_line_number,
    cast(substring(gl_extension_code, 1, 2) as text(2)) as gl_extension_code,
    cast(substring(source_business_unit_code_old, 1, 255) as text(255))
        as source_business_unit_code_old,
    cast(substring(source_business_unit_code, 1, 255) as text(255))
        as source_business_unit_code,
    cast(substring(void_flag, 1, 4) as text(4)) as void_flag,
    cast(payment_trans_date as timestamp_ntz(9)) as payment_trans_date,
    cast(substring(document_pay_item, 1, 6) as text(6)) as document_pay_item,
    cast(substring(payment_number, 1, 255) as text(255)) as payment_number,
    cast(substring(payment_identifier, 1, 255) as text(255))
        as payment_identifier,
    cast(substring(base_currency, 1, 20) as text(20)) as base_currency,
    cast(substring(target_account_identifier, 1, 255) as text(255))
        as target_account_identifier,
    cast(account_guid as text(255)) as account_guid,
    cast(substring(source_ledger_type, 1, 255) as text(255))
        as source_ledger_type,
    cast(substring(ledger_type, 1, 255) as text(255)) as ledger_type,
    cast(substring(gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,
    cast(substring(batch_number, 1, 255) as text(255)) as batch_number,
    cast(substring(batch_type, 1, 4) as text(4)) as batch_type,
    cast(batch_date as timestamp_ntz(9)) as batch_date,
    cast(substring(source_address_number, 1, 255) as text(255))
        as source_address_number,
    cast(address_guid as text(255)) as address_guid,
    cast(substring(company_code, 1, 20) as text(20)) as company_code,
    cast(substring(source_object_id_old, 1, 255) as text(255))
        as source_object_id_old,
    cast(substring(source_object_id, 1, 255) as text(255)) as source_object_id,
    cast(substring(source_subsidary_id, 1, 255) as text(255))
        as source_subsidiary_id,
    cast(substring(source_company_code, 1, 255) as text(255))
        as source_company_code,
    cast(business_unit_address_guid_old as text(255))
        as business_unit_address_guid_old,
    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,
    cast(txn_ledger_amt as number(38, 10)) as txn_ledger_amt,
    cast(substring(transaction_currency, 1, 20) as text(20))
        as transaction_currency,
    cast(base_ledger_amt as number(38, 10)) as base_ledger_amt,
    cast(quantity as number(38, 10)) as quantity,
    cast(substring(supplier_invoice_number, 1, 255) as text(255))
        as supplier_invoice_number,
    cast(invoice_date as timestamp_ntz(9)) as invoice_date,
    cast(substring(account_cat21_code, 1, 255) as text(255))
        as account_cat21_code,
    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(substring(explanation_txt, 1, 255) as text(255)) as explanation_txt,
    cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,
    cast(substring(reference1_txt, 1, 255) as text(255)) as reference1_txt,
    cast(substring(reference2_txt, 1, 255) as text(255)) as reference2_txt,
    cast(substring(reference3_txt, 1, 255) as text(255)) as reference3_txt,
    cast(substring(transaction_uom, 1, 255) as text(255)) as transaction_uom,
    cast(substring(department, 1, 30) as text(30)) as department,
    cast(substring(caf_no, 1, 30) as text(30)) as caf_no,
    cast(substring(plant, 1, 30) as text(30)) as plant,
    cast(phi_ledger_amt as number(38, 10)) as phi_ledger_amt,
    cast(pcomp_ledger_amt as number(38, 10)) as pcomp_ledger_amt,
    cast(txn_conv_rt as number(38, 10)) as txn_conv_rt,
    cast(substring(phi_currency, 1, 10) as text(10)) as phi_currency,
    cast(phi_conv_rt as number(38, 10)) as phi_conv_rt,
    cast(substring(pcomp_currency, 1, 10) as text(10)) as pcomp_currency,
    cast(pcomp_conv_rt as number(38, 10)) as pcomp_conv_rt,
    cast(substring(product_dim, 1, 255) as text(255)) as product_dim,
    cast(substring(customer_dim, 1, 255) as text(255)) as customer_dim,
    cast(oc_txn_ledger_amt as number(38, 10)) as oc_txn_ledger_amt,
    cast(oc_base_ledger_amt as number(38, 10)) as oc_base_ledger_amt,
    cast(oc_pcomp_ledger_amt as number(38, 10)) as oc_pcomp_ledger_amt,
    cast(oc_phi_ledger_amt as number(38, 10)) as oc_phi_ledger_amt,
    cast(substring(journal_category, 1, 20) as text(20)) as journal_category,
    cast(source_legacy as text(15)) as source_legacy
from unique_key_regen