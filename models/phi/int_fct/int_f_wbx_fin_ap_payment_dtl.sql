{{ config(tags=["finance", "ap"]) }}

with
    src as (select * from {{ ref("stg_f_wbx_fin_ap_payment_dtl") }}),

    dim_date as (select calendar_date, calendar_date_id from {{ ref("src_dim_date") }}),

    dim_wbx_account as (
        select
            upper(source_system) as source_system,
            account_guid as account_guid,
            source_company_code as source_company_code,
            source_account_identifier as source_account_identifier,
            consolidation_account as consolidation_account
        from {{ ref("dim_wbx_account") }}
    ),

    currency_exch_rate_dly_dim_oc as (
        select
            curr_from_code as curr_from_code,
            curr_to_code as curr_to_code,
            curr_conv_rt as curr_conv_rt,
            eff_from_d as eff_from_d,
            source_system as source_system
        from {{ ref("dim_wbx_exchange_rate_dly_oc") }}
        /* 29-May-2023: change from from src_currency_exch_rate_dly_dim_oc to dim_wbx_exchange_rate_dly_oc */
    ),

    ref_effective_currency_dim as (
        select distinct
            source_system as source_system,
            company_code as company_code,
            company_default_currency_code as company_default_currency_code,
            parent_currency_code as parent_currency_code,
            effective_date as effective_date,
            expiration_date as expiration_date
        from {{ ref("src_ref_effective_currency_dim") }}
    ),

    ref_payment_terms_xref as (
        select
            upper(source_system) as source_system,
            payment_terms_guid as payment_terms_guid,
            source_payment_terms_code as source_payment_terms_code,
            days_to_pay as days_to_pay,
            days_to_discount as days_to_discount
        from {{ ref("xref_wbx_payment_terms") }}
    ),

    wbx_voucher_hdr_fact as (
        select unique_key from {{ ref("fct_wbx_fin_ap_voucher_hdr") }}
    ),

    conv_rt as (
        select
            src.company_code
            || '.'
            || src.source_business_unit_code
            || '.'
            || src.source_object_code as source_concat_nat_key,
            {{
                dbt_utils.surrogate_key(
                    ["src.source_system", "source_concat_nat_key"]
                )
            }} as account_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "src.source_system",
                        "src.source_payee_identifier",
                        "'SUPPLIER'",
                    ]
                )
            }} as payee_address_number_guid,
            src.source_system as source_system,
            src.payment_identifier,
            cast(src.line_number as number(38, 10)) as line_number,
            src.document_company,
            nvl(
                to_char(document_type_lkp.normalized_value), to_char(src.document_type)
            ) as document_type,
            src.document_number,
            src.document_pay_item,
            src.document_pay_item_ext,
            src.source_business_unit_code,
            case
                when (src.source_business_unit_code) = '-'
                then '0'
                else
                    {{
                        dbt_utils.surrogate_key(
                            [
                                "src.source_system",
                                "src.source_business_unit_code",
                                "'PLANT_DC'",
                            ]
                        )
                    }}
            end as business_unit_address_guid,
            src.source_payee_identifier,
            src.source_account_identifier,
            dim_wbx_account.consolidation_account as target_account_identifier,
            src.source_payment_instr_code,
            to_char(
                source_payment_instr_desc_lkp.normalized_value
            ) as payment_instr_desc,
            /*nvl(
                to_char(source_payment_instr_desc_lkp.normalized_value),
                to_char(src.source_payment_instr_code)
            ) as payment_instr_desc,*/
            to_char(
                source_payment_instr_code_lkp.normalized_value
            ) as target_payment_instr_code,
            /*nvl(
                to_char(source_payment_instr_code_lkp.normalized_value),
                to_char(src.source_payment_instr_code)
            ) as target_payment_instr_code,*/
            src.gl_posted_flag,
            src.purchase_order_number,
            src.remark_txt,
            nvl(
                to_char(txn_currency_lkp.normalized_value), to_char(src.txn_currency)
            ) as transaction_currency,
            ref_effective_currency_dim.company_default_currency_code as base_currency,
            ref_effective_currency_dim.parent_currency_code as pcomp_currency,
            'USD' as phi_currency,
            case
                when
                    ref_effective_currency_dim.company_default_currency_code = nvl(
                        to_char(txn_currency_lkp.normalized_value),
                        to_char(src.txn_currency)
                    )
                then 1
                else coalesce(txn_conv_rt_lkp.curr_conv_rt, 0)
            end as txn_conv_rt,
            '1' as base_conv_rt,
            case
                when ref_effective_currency_dim.company_default_currency_code = 'USD'
                then 1
                else coalesce(phi_conv_rt_lkp.curr_conv_rt, 0)
            end as phi_conv_rt,
            case
                when
                    ref_effective_currency_dim.company_default_currency_code
                    = ref_effective_currency_dim.parent_currency_code
                then 1
                else coalesce(pcomp_conv_rt_lkp.curr_conv_rt, 0)
            end as pcomp_conv_rt,
            src.txn_payment_amt,
            src.txn_discount_available_amt,
            src.txn_discount_taken_amt,
            src.base_payment_amt,
            src.base_discount_available_amt,
            src.base_discount_taken_amt,
            src.payment_trans_doc_number,
            src.payment_trans_date,
            src.void_date,
            src.payment_trans_cleared_date,
            src.void_flag,
            src.company_code,
            src.source_updated_datetime as source_date_updated,
            trunc(dim_date.calendar_date_id) as source_updated_d_id,
            systimestamp() as load_date,
            systimestamp() as update_date
        from src
        left join
            dim_date
            on dim_date.calendar_date
            = to_char(src.source_updated_datetime, 'yyyy-mm-dd')

        left join
            dim_wbx_account
            on dim_wbx_account.source_system = src.source_system
            and dim_wbx_account.source_account_identifier
            = src.source_account_identifier
            and dim_wbx_account.source_company_code = src.company_code


        left join
            ref_effective_currency_dim
            on ref_effective_currency_dim.source_system = src.source_system
            and ref_effective_currency_dim.company_code = src.company_code
            and ref_effective_currency_dim.effective_date <= src.payment_trans_date
            and ref_effective_currency_dim.expiration_date >= src.payment_trans_date

        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "FINANCE",
                    "DOCUMENT_TYPE_CODE",
                    "UPPER(SRC.DOCUMENT_TYPE)",
                    "DOCUMENT_TYPE_LKP",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "FINANCE",
                    "PAYMENT_TYPE_CODE",
                    "UPPER(SRC.source_payment_instr_code)",
                    "source_payment_instr_code_LKP",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "FINANCE",
                    "PAYMENT_TYPE_DESC",
                    "UPPER(SRC.source_payment_instr_code)",
                    "source_payment_instr_desc_LKP",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "ADDRESS_BOOK",
                    "SUPP_CURRENCY_CODE",
                    "UPPER(SRC.txn_currency)",
                    "txn_currency_LKP",
                )
            }}

        left join
            {{
                lkp_exchange_rate_daily_oc(
                    "SRC.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "NVL(TO_CHAR(txn_currency_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.txn_currency))",
                    "SRC.payment_trans_date",
                    "txn_conv_rt_LKP",
                )
            }}

        left join
            {{
                lkp_exchange_rate_daily_oc(
                    "SRC.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "'USD'",
                    "SRC.payment_trans_date",
                    "phi_conv_rt_LKP",
                )
            }}

        left join
            {{
                lkp_exchange_rate_daily_oc(
                    "SRC.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "ref_effective_currency_dim.parent_currency_code",
                    "SRC.payment_trans_date",
                    "pcomp_conv_rt_LKP",
                )
            }}
    ),

    guidgen as (
        select
            conv_rt.*,
            {{
                dbt_utils.surrogate_key(
                    [
                        "conv_rt.source_system",
                        "conv_rt.document_company",
                        "conv_rt.document_number",
                        "conv_rt.document_type",
                    ]
                )
            }} as ap_voucher_hdr_unique_key
        from conv_rt
    ),

    joiner as (
        select guidgen.*, wbx_voucher_hdr_fact.unique_key as voucher_hdr_unique_key
        from guidgen
        left join
            wbx_voucher_hdr_fact
            on guidgen.ap_voucher_hdr_unique_key = wbx_voucher_hdr_fact.unique_key
    ),

    filterflag as (
        select
            joiner.*,
            case
                when
                    joiner.voucher_hdr_unique_key is null
                    and substring(joiner.payment_identifier, 1, 3) not in ('NIN')
                    and substring(joiner.document_number, 1, 3) not in ('PDD', 'PBB')
                then 'Y'
                else 'N'
            end as filter_flag
        from joiner
    ),

    datafilter as (
        select filterflag.* from filterflag where filterflag.filter_flag = 'N'
    )

select

    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(substring(payment_identifier, 1, 255) as text(255)) as payment_identifier,

    cast(line_number as number(38, 10)) as line_number,

    cast(substring(document_company, 1, 255) as text(255)) as document_company,

    cast(substring(document_type, 1, 255) as text(255)) as document_type,

    cast(substring(document_number, 1, 255) as text(255)) as document_number,

    cast(substring(document_pay_item, 1, 255) as text(255)) as document_pay_item,

    cast(substring(document_pay_item_ext, 1, 6) as text(6)) as document_pay_item_ext,

    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,

    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,

    cast(
        substring(source_payee_identifier, 1, 255) as text(255)
    ) as source_payee_identifier,

    cast(payee_address_number_guid as text(255)) as payee_address_number_guid,

    cast(
        substring(source_account_identifier, 1, 255) as text(255)
    ) as source_account_identifier,

    cast(
        substring(target_account_identifier, 1, 255) as text(255)
    ) as target_account_identifier,

    cast(account_guid as text(255)) as account_guid,

    cast(
        substring(source_payment_instr_code, 1, 255) as text(255)
    ) as source_payment_instr_code,

    cast(
        substring(target_payment_instr_code, 1, 255) as text(255)
    ) as target_payment_instr_code,

    cast(substring(payment_instr_desc, 1, 255) as text(255)) as payment_instr_desc,

    cast(substring(gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,

    cast(
        substring(purchase_order_number, 1, 255) as text(255)
    ) as purchase_order_number,

    cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,

    cast(substring(transaction_currency, 1, 255) as text(255)) as transaction_currency,

    cast(substring(base_currency, 1, 255) as text(255)) as base_currency,

    cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,

    cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,

    cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,

    cast(base_conv_rt as number(29, 9)) as base_conv_rt,

    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,

    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,

    cast(txn_payment_amt as number(38, 10)) as txn_payment_amt,

    cast(txn_discount_available_amt as number(38, 10)) as txn_discount_available_amt,

    cast(txn_discount_taken_amt as number(38, 10)) as txn_discount_taken_amt,

    cast(base_payment_amt as number(38, 10)) as base_payment_amt,

    cast(base_discount_available_amt as number(38, 10)) as base_discount_available_amt,

    cast(base_discount_taken_amt as number(38, 10)) as base_discount_taken_amt,

    cast(base_payment_amt * phi_conv_rt as number(38, 10)) as phi_payment_amt,

    cast(
        coalesce(base_discount_available_amt * phi_conv_rt, 0) as number(38, 10)
    ) as phi_discount_available_amt,

    cast(
        coalesce(base_discount_taken_amt * phi_conv_rt, 0) as number(38, 10)
    ) as phi_discount_taken_amt,

    cast(
        coalesce(base_payment_amt * pcomp_conv_rt, 0) as number(38, 10)
    ) as pcomp_payment_amt,

    cast(
        coalesce(base_discount_available_amt * pcomp_conv_rt, 0) as number(38, 10)
    ) as pcomp_discount_available_amt,

    cast(
        coalesce(base_discount_taken_amt * pcomp_conv_rt, 0) as number(38, 10)
    ) as pcomp_discount_taken_amt,

    cast(
        substring(payment_trans_doc_number, 1, 255) as text(255)
    ) as payment_trans_doc_number,

    cast(payment_trans_date as timestamp_ntz(9)) as payment_trans_date,

    cast(void_date as timestamp_ntz(9)) as void_date,

    cast(payment_trans_cleared_date as timestamp_ntz(9)) as payment_trans_cleared_date,

    cast(substring(void_flag, 1, 4) as text(4)) as void_flag,

    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,

    cast(load_date as timestamp_ntz(9)) as load_date,

    cast(update_date as timestamp_ntz(9)) as update_date,

    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,

    cast(ap_voucher_hdr_unique_key as text(255)) as ap_voucher_hdr_unique_key,

    cast(
        {{
            dbt_utils.surrogate_key(
                [
                    "datafilter.source_system",
                    "datafilter.payment_identifier",
                    "datafilter.line_number",
                    "datafilter.document_number",
                    "datafilter.document_type",
                    "datafilter.document_company",
                    "datafilter.document_pay_item",
                    "datafilter.document_pay_item_ext",
                ]
            )
        }} as text(255)
    ) as unique_key

from datafilter
