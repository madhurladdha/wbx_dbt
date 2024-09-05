{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_finance_po"]
    )
}}

with
old_fct as (
    select *
    from {{ source("WBX_PROD_FACT", "fct_wbx_fin_prc_po_receipt") }}
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
        po_order_number,
        po_order_type,
        po_receipt_match_type,
        po_number_of_lines,
        po_line_number,
        po_order_suffix,
        po_order_company,
        document_number,
        line_type,
        document_type,
        document_company,
        document_pay_item,
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
        acnt.source_account_identifier as source_account_identifier,
        a.source_account_identifier as source_account_identifier_old,
        target_account_identifier,
        acnt.account_guid,
        a.account_guid as account_guid_old,
        source_item_identifier,
        item_guid,
        source_subledger_identifier,
        source_subledger_type,
        pay_status_code,
        line_status,
        po_order_date,
        po_received_date,
        po_requested_date,
        po_promised_dlv_date,
        gl_date,
        transaction_uom,
        receipt_order_quantity,
        receipt_paidtodate_quantity,
        receipt_open_quantity,
        receipt_received_quantity,
        receipt_closed_quantity,
        receipt_stocked_quantity,
        receipt_returned_quantity,
        receipt_reworked_quantity,
        receipt_scrapped_quantity,
        receipt_rejected_quantity,
        receipt_adjusted_quantity,
        receipt_unit_cost,
        supplier_invoice_number,
        gl_offset_srccd,
        source_date_updated,
        load_date,
        update_date,
        source_updated_d_id,
        txn_currency,
        base_currency,
        phi_currency,
        txn_conv_rt,
        phi_conv_rt,
        base_receipt_paidtodate_amt,
        base_receipt_open_amt,
        base_receipt_received_amt,
        base_receipt_closed_amt,
        txn_receipt_paidtodate_amt,
        txn_receipt_open_amt,
        txn_receipt_received_amt,
        txn_receipt_closed_amt,
        phi_receipt_paidtodate_amt,
        phi_receipt_open_amt,
        phi_receipt_received_amt,
        phi_receipt_closed_amt,
        base_receipt_cost_variance,
        txn_receipt_cost_variance,
        phi_receipt_cost_variance,
        pcomp_currency,
        pcomp_conv_rt,
        pcomp_receipt_paidtodate_amt,
        pcomp_receipt_open_amt,
        pcomp_receipt_received_amt,
        pcomp_receipt_closed_amt,
        pcomp_receipt_cost_variance,
        base_receipt_unit_cost,
        txn_receipt_unit_cost,
        phi_receipt_unit_cost,
        pcomp_receipt_unit_cost,
        receipt_freight_amt,
        contract_agreement_guid,
        po_fact_unique_key,
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
    cast(substring(po_order_number, 1, 255) as text(255)) as po_order_number,
    cast(substring(po_order_type, 1, 20) as text(20)) as po_order_type,
    cast(
        substring(po_receipt_match_type, 1, 6) as text(6)
    ) as po_receipt_match_type,
    cast(po_number_of_lines as number(38, 10)) as po_number_of_lines,
    cast(po_line_number as number(38, 10)) as po_line_number,
    cast(substring(po_order_suffix, 1, 6) as text(6)) as po_order_suffix,
    cast(substring(po_order_company, 1, 20) as text(20)) as po_order_company,
    cast(substring(document_number, 1, 255) as text(255)) as document_number,
    cast(substring(line_type, 1, 255) as text(255)) as line_type,
    cast(substring(document_type, 1, 20) as text(20)) as document_type,
    cast(substring(document_company, 1, 20) as text(20)) as document_company,
    cast(
        substring(document_pay_item, 1, 255) as text(255)
    ) as document_pay_item,
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
    case
        when cast(substring(source_subledger_type, 1, 255) as text(255)) = '-'
            then '_'
        else cast(substring(source_subledger_type, 1, 255) as text(255))
    end as source_subledger_type,
    cast(substring(pay_status_code, 1, 20) as text(20)) as pay_status_code,
    cast(substring(line_status, 1, 255) as text(255)) as line_status,
    cast(po_order_date as date) as po_order_date,
    cast(po_received_date as date) as po_received_date,
    cast(po_requested_date as date) as po_requested_date,
    cast(po_promised_dlv_date as date) as po_promised_dlv_date,
    cast(gl_date as date) as gl_date,
    cast(substring(transaction_uom, 1, 20) as text(20)) as transaction_uom,
    cast(receipt_order_quantity as number(38, 10)) as receipt_order_quantity,
    cast(
        receipt_paidtodate_quantity as number(38, 10)
    ) as receipt_paidtodate_quantity,
    cast(receipt_open_quantity as number(38, 10)) as receipt_open_quantity,
    cast(
        receipt_received_quantity as number(38, 10)
    ) as receipt_received_quantity,
    cast(receipt_closed_quantity as number(38, 10)) as receipt_closed_quantity,
    cast(
        receipt_stocked_quantity as number(38, 10)
    ) as receipt_stocked_quantity,
    cast(
        receipt_returned_quantity as number(38, 10)
    ) as receipt_returned_quantity,
    cast(
        receipt_reworked_quantity as number(38, 10)
    ) as receipt_reworked_quantity,
    cast(
        receipt_scrapped_quantity as number(38, 10)
    ) as receipt_scrapped_quantity,
    cast(
        receipt_rejected_quantity as number(38, 10)
    ) as receipt_rejected_quantity,
    cast(
        receipt_adjusted_quantity as number(38, 10)
    ) as receipt_adjusted_quantity,
    cast(receipt_unit_cost as number(38, 10)) as receipt_unit_cost,
    cast(
        substring(supplier_invoice_number, 1, 255) as text(255)
    ) as supplier_invoice_number,
    cast(substring(gl_offset_srccd, 1, 20) as text(20)) as gl_offset_srccd,
    cast(source_date_updated as date) as source_date_updated,
    cast(load_date as timestamp_ntz(6)) as load_date,
    cast(update_date as timestamp_ntz(6)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(substring(txn_currency, 1, 10) as text(10)) as txn_currency,
    cast(substring(base_currency, 1, 10) as text(10)) as base_currency,
    cast(substring(phi_currency, 1, 10) as text(10)) as phi_currency,
    cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,
    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
    cast(
        base_receipt_paidtodate_amt as number(38, 10)
    ) as base_receipt_paidtodate_amt,
    cast(base_receipt_open_amt as number(38, 10)) as base_receipt_open_amt,
    cast(
        base_receipt_received_amt as number(38, 10)
    ) as base_receipt_received_amt,
    cast(base_receipt_closed_amt as number(38, 10)) as base_receipt_closed_amt,
    cast(
        txn_receipt_paidtodate_amt as number(38, 10)
    ) as txn_receipt_paidtodate_amt,
    cast(txn_receipt_open_amt as number(38, 10)) as txn_receipt_open_amt,
    cast(
        txn_receipt_received_amt as number(38, 10)
    ) as txn_receipt_received_amt,
    cast(txn_receipt_closed_amt as number(38, 10)) as txn_receipt_closed_amt,
    cast(
        phi_receipt_paidtodate_amt as number(38, 10)
    ) as phi_receipt_paidtodate_amt,
    cast(phi_receipt_open_amt as number(38, 10)) as phi_receipt_open_amt,
    cast(
        phi_receipt_received_amt as number(38, 10)
    ) as phi_receipt_received_amt,
    cast(phi_receipt_closed_amt as number(38, 10)) as phi_receipt_closed_amt,
    cast(
        base_receipt_cost_variance as number(38, 10)
    ) as base_receipt_cost_variance,
    cast(
        txn_receipt_cost_variance as number(38, 10)
    ) as txn_receipt_cost_variance,
    cast(
        phi_receipt_cost_variance as number(38, 10)
    ) as phi_receipt_cost_variance,
    cast(substring(pcomp_currency, 1, 10) as text(10)) as pcomp_currency,
    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
    cast(
        pcomp_receipt_paidtodate_amt as number(38, 10)
    ) as pcomp_receipt_paidtodate_amt,
    cast(pcomp_receipt_open_amt as number(38, 10)) as pcomp_receipt_open_amt,
    cast(
        pcomp_receipt_received_amt as number(38, 10)
    ) as pcomp_receipt_received_amt,
    cast(
        pcomp_receipt_closed_amt as number(38, 10)
    ) as pcomp_receipt_closed_amt,
    cast(
        pcomp_receipt_cost_variance as number(38, 10)
    ) as pcomp_receipt_cost_variance,
    cast(base_receipt_unit_cost as number(38, 10)) as base_receipt_unit_cost,
    cast(txn_receipt_unit_cost as number(38, 10)) as txn_receipt_unit_cost,
    cast(phi_receipt_unit_cost as number(38, 10)) as phi_receipt_unit_cost,
    cast(pcomp_receipt_unit_cost as number(38, 10)) as pcomp_receipt_unit_cost,
    cast(receipt_freight_amt as number(38, 10)) as receipt_freight_amt,
    cast(contract_agreement_guid as text(255)) as contract_agreement_guid,
    cast(po_fact_unique_key as text(255)) as po_fact_unique_key,
    cast(substring(agreement_number, 1, 255) as text(255)) as agreement_number,
    cast(unique_key as text(255)) as unique_key
from converted_fct