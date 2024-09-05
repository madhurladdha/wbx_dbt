{{ config(tags=["finance", "po"]) }}

with
    src as (select * from {{ ref("stg_f_wbx_fin_prc_po_receipt") }}),

    dim_date as (select calendar_date, calendar_date_id from {{ ref("src_dim_date") }}),

    dim_wbx_account as (
        select
            upper(source_system) as source_system,
            account_guid as account_guid,
            source_company_code as source_company_code,
            source_account_identifier as source_account_identifier,
            source_business_unit_code as source_business_unit_code,
            tagetik_account as tagetik_account
        from {{ ref("dim_wbx_account") }}
    ),

    ref_effective_currency_dim as (
        select distinct
            source_system as source_system,
            company_code as company_code,
            company_default_currency_code as company_default_currency_code,
            parent_currency_code as parent_currency_code,
            effective_date as effective_date,
            expiration_date as expiration_date,
            company_address_guid
        from {{ ref("src_ref_effective_currency_dim") }}
    ),

    dim_company as (
        select distinct
            ltrim(rtrim(a.source_system)) as source_system,
            ltrim(rtrim(a.company_code)) as company_code,
            ltrim(rtrim(b.company_default_currency_code)) as default_currency_code,
            ltrim(rtrim(b.parent_currency_code)) as parent_currency_code,
            b.effective_date as effective_date,
            b.expiration_date as expiration_date
        from {{ ref("dim_wbx_company") }} a, ref_effective_currency_dim b
        where a.company_address_guid = b.company_address_guid
    ),

    wbx_prc_po_fact as (
        select unique_key, contract_agreement_guid from {{ ref("fct_wbx_fin_prc_po") }}
    ),

    source as (
        select
            src.po_receipt_match_type,
            iff(
                src.po_order_company is null, '0', ltrim(rtrim(src.po_order_company))
            ) as po_order_company,
            src.po_order_number,
            ltrim(rtrim(upper(src.po_order_type))) as source_po_order_type,
            src.po_order_suffix,
            cast(src.po_line_number as number(38, 10)) as po_line_number,
            cast(src.po_number_of_lines as number(38, 10)) as po_number_of_lines,
            src.document_number,
            src.source_item_identifier,
            ltrim(rtrim(src.source_supplier_identifier)) as source_supplier_identifier,
            cast(src.po_order_date as string(255)) as po_order_date,
            cast(src.po_received_date as string(255)) as po_received_date,
            cast(src.po_requested_date as string(255)) as po_requested_date,
            cast(src.po_promised_dlv_date as string(255)) as po_promised_dlv_date,
            src.supplier_invoice_number,
            cast(
                iff(
                    ltrim(rtrim(src.source_payment_terms_code)) = ''
                    or src.source_payment_terms_code is null,
                    '-',
                    ltrim(rtrim(src.source_payment_terms_code))
                ) as varchar(255)
            ) as source_payment_terms_code,
            ltrim(rtrim(upper(src.pay_status_code))) as source_pay_status_code,
            ltrim(rtrim(upper(src.line_status))) as source_line_status,
            ltrim(rtrim(upper(src.line_type))) as source_line_type,
            src.gl_offset_srccd as gl_offset_srccd,
            src.source_business_unit_code,
            ltrim(rtrim(src.source_account_identifier)) as source_account_identifier,
            ltrim(
                rtrim(src.source_subledger_identifier)
            ) as source_subledger_identifier,
            ltrim(rtrim(src.source_subledger_type)) as source_subledger_type,
            ltrim(rtrim(src.document_company)) as document_company,
            ltrim(rtrim(upper(src.document_type))) as source_document_type,
            src.document_pay_item,
            cast(src.po_gl_date as string(255)) as gl_date,
            src.receipt_order_quantity,
            src.receipt_paidtodate_quantity,
            src.receipt_open_quantity,
            src.receipt_received_quantity,
            src.receipt_closed_quantity,
            src.receipt_stocked_quantity,
            src.receipt_returned_quantity,
            src.receipt_reworked_quantity,
            src.receipt_scrapped_quantity,
            src.receipt_rejected_quantity,
            src.receipt_adjusted_quantity,
            src.transaction_uom as source_transaction_uom,
            src.receipt_unit_cost,
            src.receipt_paidtodate_amt,
            src.receipt_open_amt,
            src.receipt_received_amt,
            src.receipt_closed_amt,
            0 as receipt_cost_variance,
            ltrim(
                rtrim(upper(src.transaction_currency))
            ) as source_transaction_currency,
            cast(src.source_date_updated as string(255)) as source_date_updated,
            1 as source_updated_d_id,
            ltrim(rtrim(src.source_system)) as source_system,
            src.rma_number,
            src.agreement_number,
            source_cost_center,
            0 as receipt_freight_amt,
            systimestamp() as load_date,
            systimestamp() as update_date,
            src.po_order_company
            || '.'
            || src.source_cost_center
            || '.'
            || src.source_object_code as source_concat_nat_key,
            {{
                dbt_utils.surrogate_key(
                    ["src.source_system", "source_concat_nat_key"]
                )
            }} as account_guid,
            dim_wbx_account.tagetik_account as target_account_identifier
        from src
        left join
            dim_wbx_account
            on dim_wbx_account.source_system = src.source_system
            and dim_wbx_account.source_account_identifier
            = src.source_account_identifier
            and dim_wbx_account.source_company_code = src.po_order_company
            and dim_wbx_account.source_business_unit_code = src.source_cost_center
    ),

    lkp_guid_normalization as (
        select
            source.*,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source.source_system",
                        "ltrim(rtrim(source.source_business_unit_code))",
                        "'PLANT_DC'",
                    ]
                )
            }} as business_unit_address_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source.source_system",
                        "source.source_supplier_identifier",
                        "'SUPPLIER'",
                        "po_order_company",
                    ]
                )
            }} as supplier_address_number_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source.source_system",
                        "ltrim(rtrim(source.source_item_identifier))",
                    ]
                )
            }} as item_guid,
            {{
                dbt_utils.surrogate_key(
                    ["source.source_system", "source.source_payment_terms_code"]
                )
            }} as payment_terms_guid,
            coalesce(
                ref_normalization_xref_trans_uom.normalized_value,
                source_transaction_uom
            ) as transaction_uom,
            ref_normalization_trans_curr.normalized_value as transaction_currency,
            iff(
                ref_normalization_xref_po_ord_type.normalized_value is null,
                ltrim(rtrim(source_po_order_type)),
                ref_normalization_xref_po_ord_type.normalized_value
            ) as po_order_type,
            iff(
                ref_normalization_xref_line_type.normalized_value is null,
                ltrim(rtrim(source_line_type)),
                ref_normalization_xref_line_type.normalized_value
            ) as line_type,
            iff(
                ref_normalization_xref_pay_status.normalized_value is null,
                ltrim(rtrim(source_pay_status_code)),
                ref_normalization_xref_pay_status.normalized_value
            ) as pay_status_code,
            iff(
                ref_normalization_xref_doc_type.normalized_value is null,
                ltrim(rtrim(source_document_type)),
                ref_normalization_xref_doc_type.normalized_value
            ) as document_type,
            iff(
                ref_normalization_xref_line_status.normalized_value is null,
                ltrim(rtrim(source_line_status)),
                ref_normalization_xref_line_status.normalized_value
            ) as line_status
        from source
        left join
            {{
                lkp_normalization(
                    "source.source_system",
                    "ITEM",
                    "PRIMARY_UOM",
                    "ltrim(rtrim(source_transaction_uom))",
                    "ref_normalization_xref_trans_uom",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "source.source_system",
                    "ADDRESS_BOOK",
                    "SUPP_CURRENCY_CODE",
                    "TRIM(source_transaction_currency)",
                    "ref_normalization_trans_curr",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "source.source_system",
                    "FINANCE",
                    "PO_DOC_TYPE_CODE",
                    "trim(source_po_order_type)",
                    "ref_normalization_xref_po_ord_type",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "source.source_system",
                    "FINANCE",
                    "LINE_TYPE_CODE",
                    "trim(source_line_type)",
                    "ref_normalization_xref_line_type",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "source.source_system",
                    "ADDRESS_BOOK",
                    "PAY_STATUS_CODE",
                    "ltrim(rtrim(upper(source_pay_status_code)))",
                    "ref_normalization_xref_pay_status",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "source.source_system",
                    "FINANCE",
                    "DOCUMENT_TYPE_CODE",
                    "trim(source_document_type)",
                    "ref_normalization_xref_doc_type",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "source.source_system",
                    "FINANCE",
                    "LINE_STATUS_DESC",
                    "trim(source_line_status)",
                    "ref_normalization_xref_line_status",
                )
            }}

    ),

    exp_conv as (
        select
            lkp_guid_normalization.*,
            add_months(lkp_guid_normalization.gl_date, -1) as prev_mnth,
            last_day(prev_mnth) as last_day_of_prev_month,
            adr_company_master_dim_po_order_date.default_currency_code as base_currency,
            'USD' as phi_currency,
            adr_company_master_dim_po_order_date.parent_currency_code as pcomp_currency
        from lkp_guid_normalization
        left join
            dim_company as adr_company_master_dim_po_order_date
            on adr_company_master_dim_po_order_date.source_system
            = lkp_guid_normalization.source_system
            and adr_company_master_dim_po_order_date.company_code
            = lkp_guid_normalization.po_order_company
            and adr_company_master_dim_po_order_date.effective_date <= po_order_date
            and adr_company_master_dim_po_order_date.expiration_date >= po_order_date

    ),

    exp_rt_calc as (
        select
            exp_conv.*,
            case
                when base_currency = transaction_currency
                then 1
                else coalesce(trans.curr_conv_rt, 0)
            end as txn_conv_rt,
            case
                when base_currency = phi_currency
                then 1
                else coalesce(phi.curr_conv_rt, 0)
            end as phi_conv_rt,
            1 as base_conv_rt,
            case
                when base_currency = pcomp_currency
                then 1
                else coalesce(pcom.curr_conv_rt, 0)
            end as pcomp_conv_rt,
            case
                when txn_conv_rt <> 0 then receipt_unit_cost * (1 / txn_conv_rt) else 0
            end as base_receipt_unit_cost,
            case
                when txn_conv_rt <> 0
                then receipt_paidtodate_amt * (1 / txn_conv_rt)
                else 0
            end as base_receipt_paidtodate_amt,
            case
                when txn_conv_rt <> 0 then receipt_open_amt * (1 / txn_conv_rt) else 0
            end as base_receipt_open_amt,
            case
                when txn_conv_rt <> 0
                then receipt_received_amt * (1 / txn_conv_rt)
                else 0
            end as base_receipt_received_amt,
            case
                when txn_conv_rt <> 0 then receipt_closed_amt * (1 / txn_conv_rt) else 0
            end as base_receipt_closed_amt,
            case
                when txn_conv_rt <> 0
                then receipt_cost_variance * (1 / txn_conv_rt)
                else 0
            end as base_receipt_cost_variance,
            base_receipt_paidtodate_amt * txn_conv_rt as txn_receipt_paidtodate_amt,
            base_receipt_open_amt * txn_conv_rt as txn_receipt_open_amt,
            base_receipt_unit_cost * txn_conv_rt as txn_receipt_unit_cost,
            base_receipt_received_amt * txn_conv_rt as txn_receipt_received_amt,
            base_receipt_closed_amt * txn_conv_rt as txn_receipt_closed_amt,
            base_receipt_paidtodate_amt * phi_conv_rt as phi_receipt_paidtodate_amt,
            base_receipt_open_amt * phi_conv_rt as phi_receipt_open_amt,
            base_receipt_unit_cost * phi_conv_rt as phi_receipt_unit_cost,
            base_receipt_received_amt * phi_conv_rt as phi_receipt_received_amt,
            base_receipt_closed_amt * phi_conv_rt as phi_receipt_closed_amt,
            base_receipt_cost_variance * txn_conv_rt as txn_receipt_cost_variance,
            base_receipt_cost_variance * phi_conv_rt as phi_receipt_cost_variance,
            base_receipt_paidtodate_amt * pcomp_conv_rt as pcomp_receipt_paidtodate_amt,
            base_receipt_open_amt * pcomp_conv_rt as pcomp_receipt_open_amt,
            base_receipt_unit_cost * pcomp_conv_rt as pcomp_receipt_unit_cost,
            base_receipt_received_amt * pcomp_conv_rt as pcomp_receipt_received_amt,
            base_receipt_closed_amt * pcomp_conv_rt as pcomp_receipt_closed_amt,
            base_receipt_cost_variance * pcomp_conv_rt as pcomp_receipt_cost_variance
        from exp_conv
        left join
            {{
                lkp_exchange_rate_daily(
                    "base_currency",
                    "transaction_currency",
                    "last_day_of_prev_month",
                    "trans",
                )
            }}
        left join
            {{
                lkp_exchange_rate_daily(
                    "base_currency", "phi_currency", "last_day_of_prev_month", "phi"
                )
            }}
        left join
            {{
                lkp_exchange_rate_daily(
                    "base_currency",
                    "pcomp_currency",
                    "last_day_of_prev_month",
                    "pcom",
                )
            }}
    ),

    unique_key_gen as (
        select
            exp_rt_calc.*,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "po_order_company",
                        "po_order_number",
                        "po_order_type",
                        "po_order_suffix",
                        "po_line_number",
                    ]
                )
            }} as po_fact_unique_key,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "po_order_number",
                        "po_order_type",
                        "po_receipt_match_type",
                        "po_number_of_lines",
                        "po_line_number",
                        "po_order_suffix",
                        "po_order_company",
                        "document_number",
                        "document_company",
                        "document_type",
                        "document_pay_item",
                    ]
                )
            }} as unique_key
        from exp_rt_calc
    ),

    joiner as (
        select
            unique_key_gen.*,
            wbx_prc_po_fact.contract_agreement_guid as contract_agreement_guid
        from unique_key_gen
        left join
            wbx_prc_po_fact
            on unique_key_gen.po_fact_unique_key = wbx_prc_po_fact.unique_key
    ),

    final as (
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
                substring(source_account_identifier, 1, 255) as text(255)
            ) as source_account_identifier,

            cast(
                substring(target_account_identifier, 1, 255) as text(255)
            ) as target_account_identifier,

            cast(account_guid as text(255)) as account_guid,

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

            cast(receipt_paidtodate_amt as number(38, 10)) as receipt_paidtodate_amt,

            cast(receipt_open_amt as number(38, 10)) as receipt_open_amt,

            cast(receipt_received_amt as number(38, 10)) as receipt_received_amt,

            cast(receipt_closed_amt as number(38, 10)) as receipt_closed_amt,

            cast(
                substring(supplier_invoice_number, 1, 255) as text(255)
            ) as supplier_invoice_number,

            cast(substring(gl_offset_srccd, 1, 20) as text(20)) as gl_offset_srccd,

            cast(source_date_updated as date) as source_date_updated,

            cast(load_date as timestamp_ntz(6)) as load_date,

            cast(update_date as timestamp_ntz(6)) as update_date,

            cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,

            cast(substring(transaction_currency, 1, 10) as text(10)) as txn_currency,

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
            cast(substring(agreement_number,1,255) as text(255) ) as agreement_number  ,

            cast(unique_key as text(255)) as unique_key

        from joiner
    )

select *
from final
