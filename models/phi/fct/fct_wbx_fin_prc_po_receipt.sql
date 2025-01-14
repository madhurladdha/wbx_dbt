{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["finance", "po"],
        transient=false,
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns",
        pre_hook="""
                    {{ truncate_if_exists(this.schema, this.table) }}
                    """,
    )
}}

with
/* Captures the main D365 transactional data set. */
src as (
    select *, row_number() over (partition by unique_key order by 1) rownum
    from {{ ref("int_f_wbx_fin_prc_po_receipt") }}
),

/* Captures the main AX history transactional data set.  This model already has any dim values converted as intended. */
old_fact as
(
    select * from {{ref('conv_fct_wbx_fin_prc_po_receipt')}}
),

d365_fact as 
(
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
    source_business_unit_code,
    business_unit_address_guid,
    source_supplier_identifier,
    supplier_address_number_guid,
    source_payment_terms_code,
    payment_terms_guid,
    source_account_identifier,
    target_account_identifier,
    account_guid,
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
    unique_key,
    'D365' as source_legacy
from src
where rownum = 1
),

axhist_fact as 
(
select
    a.source_system,
    a.po_order_number,
    a.po_order_type,
    a.po_receipt_match_type,
    a.po_number_of_lines,
    a.po_line_number,
    a.po_order_suffix,
    a.po_order_company,
    a.document_number,
    a.line_type,
    a.document_type,
    a.document_company,
    a.document_pay_item,
    a.source_business_unit_code,
    a.business_unit_address_guid,
    a.source_supplier_identifier,
    a.supplier_address_number_guid,
    a.source_payment_terms_code,
    a.payment_terms_guid,
    a.source_account_identifier,
    a.target_account_identifier,
    a.account_guid,
    a.source_item_identifier,
    a.item_guid,
    a.source_subledger_identifier,
    a.source_subledger_type,
    a.pay_status_code,
    a.line_status,
    a.po_order_date,
    a.po_received_date,
    a.po_requested_date,
    a.po_promised_dlv_date,
    a.gl_date,
    a.transaction_uom,
    a.receipt_order_quantity,
    a.receipt_paidtodate_quantity,
    a.receipt_open_quantity,
    a.receipt_received_quantity,
    a.receipt_closed_quantity,
    a.receipt_stocked_quantity,
    a.receipt_returned_quantity,
    a.receipt_reworked_quantity,
    a.receipt_scrapped_quantity,
    a.receipt_rejected_quantity,
    a.receipt_adjusted_quantity,
    a.receipt_unit_cost,
    a.supplier_invoice_number,
    a.gl_offset_srccd,
    a.source_date_updated,
    a.load_date,
    a.update_date,
    a.source_updated_d_id,
    a.txn_currency,
    a.base_currency,
    a.phi_currency,
    a.txn_conv_rt,
    a.phi_conv_rt,
    a.base_receipt_paidtodate_amt,
    a.base_receipt_open_amt,
    a.base_receipt_received_amt,
    a.base_receipt_closed_amt,
    a.txn_receipt_paidtodate_amt,
    a.txn_receipt_open_amt,
    a.txn_receipt_received_amt,
    a.txn_receipt_closed_amt,
    a.phi_receipt_paidtodate_amt,
    a.phi_receipt_open_amt,
    a.phi_receipt_received_amt,
    a.phi_receipt_closed_amt,
    a.base_receipt_cost_variance,
    a.txn_receipt_cost_variance,
    a.phi_receipt_cost_variance,
    a.pcomp_currency,
    a.pcomp_conv_rt,
    a.pcomp_receipt_paidtodate_amt,
    a.pcomp_receipt_open_amt,
    a.pcomp_receipt_received_amt,
    a.pcomp_receipt_closed_amt,
    a.pcomp_receipt_cost_variance,
    a.base_receipt_unit_cost,
    a.txn_receipt_unit_cost,
    a.phi_receipt_unit_cost,
    a.pcomp_receipt_unit_cost,
    a.receipt_freight_amt,
    a.contract_agreement_guid,
    a.po_fact_unique_key,
    a.agreement_number,
    a.unique_key,
    'AX' as source_legacy
from old_fact as a
    left join d365_fact as b on a.unique_key = b.unique_key
    where b.source_system is null
),

/* Combines (union) the D365 data w/ the AX History set.    */
combine_fact as 
(
    select * from d365_fact
    union all 
    select * from axhist_fact
)

select * from combine_fact
qualify row_number() over (partition by unique_key order by unique_key desc) = 1