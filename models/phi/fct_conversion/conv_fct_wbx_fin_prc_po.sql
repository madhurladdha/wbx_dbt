{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_finance_po"]
    )
}}

with
old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_fin_prc_po") }}
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
        payment_terms_guid,
        payment_terms_guid_old
    from {{ ref('conv_xref_wbx_payment_terms') }}
),

converted_fct as (
    select
        source_system,
        po_order_company,
        po_order_number,
        po_order_type,
        source_po_order_type,
        po_order_suffix,
        po_line_number,
        line_status,
        line_type,
        po_line_desc,
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
                        "po_order_company",
                    ]
                )
            }} as supplier_address_number_guid,
        pymt.source_payment_terms_code_new as source_payment_terms_code,
        pymt.payment_terms_guid as payment_terms_guid,
        a.source_payment_terms_code as source_payment_terms_code_old,
        a.payment_terms_guid as payment_terms_guid_old,
        source_freight_handling_code,
        source_buyer_identifier,
        buyer_address_number_guid,
        acnt.source_account_identifier as source_account_identifier,
        a.source_account_identifier as source_account_identifier_old,
        target_account_identifier,
        acnt.account_guid,
        a.account_guid as account_guid_old,
        source_item_identifier,
        item_guid,
        source_subledger_identifier,
        source_subledger_type,
        contract_company_code,
        contract_number,
        contract_type,
        source_contract_type,
        contract_line_number,
        agrmnt_number,
        agrmnt_suplmnt_number,
        po_gl_date,
        po_order_date,
        po_delivery_date,
        po_promised_delivery_date,
        po_requested_date,
        po_cancelled_date,
        base_line_unit_cost,
        line_onhold_quantity,
        line_open_quantity,
        line_order_quantity,
        line_recvd_quantity,
        transaction_uom,
        gl_offset_srccd,
        source_date_updated,
        load_date,
        update_date,
        source_updated_d_id,
        contract_agreement_flag,
        target_freight_handling_code,
        freight_handling_code_desc,
        txn_currency,
        base_currency,
        phi_currency,
        pcomp_currency,
        txn_conv_rt,
        phi_conv_rt,
        pcomp_conv_rt,
        base_order_total_amount,
        base_line_on_hold_amt,
        base_line_open_amt,
        base_line_received_amt,
        txn_order_total_amount,
        txn_line_on_hold_amt,
        txn_line_open_amt,
        txn_line_received_amt,
        phi_order_total_amount,
        phi_line_on_hold_amt,
        phi_line_open_amt,
        phi_line_received_amt,
        pcomp_order_total_amount,
        pcomp_line_on_hold_amt,
        pcomp_line_open_amt,
        pcomp_line_received_amt,
        txn_line_unit_cost,
        phi_line_unit_cost,
        pcomp_line_unit_cost,
        po_org_promise_date,
        caf_no,
        project_category,
        contract_agreement_guid,
        agreement_number,
        unique_key
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
    cast(substring(po_order_company, 1, 20) as text(20)) as po_order_company,
    cast(substring(po_order_number, 1, 255) as text(255)) as po_order_number,
    cast(substring(po_order_type, 1, 20) as text(20)) as po_order_type,
    cast(
        substring(source_po_order_type, 1, 20) as text(20)
    ) as source_po_order_type,
    cast(substring(po_order_suffix, 1, 6) as text(6)) as po_order_suffix,
    cast(po_line_number as number(38, 10)) as po_line_number,
    cast(substring(line_status, 1, 255) as text(255)) as line_status,
    cast(substring(line_type, 1, 255) as text(255)) as line_type,
    cast(substring(po_line_desc, 1, 255) as text(255)) as po_line_desc,
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
    cast(
        supplier_address_number_guid as text(255)
    ) as supplier_address_number_guid,
    cast(
        substring(source_payment_terms_code, 1, 255) as text(255)
    ) as source_payment_terms_code,
    cast(payment_terms_guid as text(255)) as payment_terms_guid,
    cast(
        substring(source_payment_terms_code_old, 1, 255) as text(255)
    ) as source_payment_terms_code_old,
    cast(payment_terms_guid_old as text(255)) as payment_terms_guid_old,
    cast(
        substring(source_freight_handling_code, 1, 255) as text(255)
    ) as source_freight_handling_code,
    cast(
        substring(source_buyer_identifier, 1, 255) as text(255)
    ) as source_buyer_identifier,
    cast(buyer_address_number_guid as text(255)) as buyer_address_number_guid,
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
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,
    cast(item_guid as text(255)) as item_guid,
    cast(
        substring(source_subledger_identifier, 1, 255) as text(255)
    ) as source_subledger_identifier,
    cast(
        substring(source_subledger_type, 1, 255) as text(255)
    ) as source_subledger_type,
    cast(
        substring(contract_company_code, 1, 20) as text(20)
    ) as contract_company_code,
    cast(substring(contract_number, 1, 255) as text(255)) as contract_number,
    cast(substring(contract_type, 1, 255) as text(255)) as contract_type,
    cast(
        substring(source_contract_type, 1, 255) as text(255)
    ) as source_contract_type,
    cast(contract_line_number as number(38, 0)) as contract_line_number,
    cast(substring(agrmnt_number, 1, 255) as text(255)) as agrmnt_number,
    cast(
        substring(agrmnt_suplmnt_number, 1, 255) as text(255)
    ) as agrmnt_suplmnt_number,
    cast(po_gl_date as timestamp_ntz(9)) as po_gl_date,
    cast(po_order_date as timestamp_ntz(9)) as po_order_date,
    cast(po_delivery_date as timestamp_ntz(9)) as po_delivery_date,
    cast(
        po_promised_delivery_date as timestamp_ntz(9)
    ) as po_promised_delivery_date,
    cast(po_requested_date as timestamp_ntz(9)) as po_requested_date,
    cast(po_cancelled_date as timestamp_ntz(9)) as po_cancelled_date,
    cast(line_onhold_quantity as number(38, 10)) as line_onhold_quantity,
    cast(line_open_quantity as number(38, 10)) as line_open_quantity,
    cast(line_order_quantity as number(38, 10)) as line_order_quantity,
    cast(line_recvd_quantity as number(38, 10)) as line_recvd_quantity,
    cast(substring(transaction_uom, 1, 20) as text(20)) as transaction_uom,
    cast(substring(gl_offset_srccd, 1, 20) as text(20)) as gl_offset_srccd,
    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(
        substring(contract_agreement_flag, 1, 2) as text(2)
    ) as contract_agreement_flag,
    cast(
        substring(target_freight_handling_code, 1, 255) as text(255)
    ) as target_freight_handling_code,
    cast(
        substring(freight_handling_code_desc, 1, 255) as text(255)
    ) as freight_handling_code_desc,
    cast(substring(txn_currency, 1, 10) as text(10)) as txn_currency,
    cast(substring(base_currency, 1, 10) as text(10)) as base_currency,
    cast(substring(phi_currency, 1, 10) as text(10)) as phi_currency,
    cast(substring(pcomp_currency, 1, 10) as text(10)) as pcomp_currency,
    cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,
    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
    cast(base_order_total_amount as number(38, 10)) as base_order_total_amount,
    cast(base_line_on_hold_amt as number(38, 10)) as base_line_on_hold_amt,
    cast(base_line_open_amt as number(38, 10)) as base_line_open_amt,
    cast(base_line_received_amt as number(38, 10)) as base_line_received_amt,
    cast(txn_order_total_amount as number(38, 10)) as txn_order_total_amount,
    cast(txn_line_on_hold_amt as number(38, 10)) as txn_line_on_hold_amt,
    cast(txn_line_open_amt as number(38, 10)) as txn_line_open_amt,
    cast(txn_line_received_amt as number(38, 10)) as txn_line_received_amt,
    cast(phi_order_total_amount as number(38, 10)) as phi_order_total_amount,
    cast(phi_line_on_hold_amt as number(38, 10)) as phi_line_on_hold_amt,
    cast(phi_line_open_amt as number(38, 10)) as phi_line_open_amt,
    cast(phi_line_received_amt as number(38, 10)) as phi_line_received_amt,
    cast(
        pcomp_order_total_amount as number(38, 10)
    ) as pcomp_order_total_amount,
    cast(pcomp_line_on_hold_amt as number(38, 10)) as pcomp_line_on_hold_amt,
    cast(pcomp_line_open_amt as number(38, 10)) as pcomp_line_open_amt,
    cast(pcomp_line_received_amt as number(38, 10)) as pcomp_line_received_amt,
    cast(base_line_unit_cost as number(38, 10)) as base_line_unit_cost,
    cast(txn_line_unit_cost as number(38, 10)) as txn_line_unit_cost,
    cast(phi_line_unit_cost as number(38, 10)) as phi_line_unit_cost,
    cast(pcomp_line_unit_cost as number(38, 10)) as pcomp_line_unit_cost,
    cast(po_org_promise_date as timestamp_ntz(9)) as po_org_promise_date,
    cast(substring(caf_no, 1, 30) as text(30)) as caf_no,
    cast(substring(project_category, 1, 30) as text(30)) as project_category,
    cast(contract_agreement_guid as text(255)) as contract_agreement_guid,
    cast(substring(agreement_number, 1, 255) as text(255)) as agreement_number,
    cast(unique_key as text(255)) as unique_key
from converted_fct