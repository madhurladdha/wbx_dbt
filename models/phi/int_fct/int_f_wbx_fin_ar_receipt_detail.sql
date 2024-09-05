{{ config(tags=["finance", "ar"]) }}

with
    src as (select * from {{ ref("stg_f_wbx_fin_ar_receipt_detail") }}),

    dim_date as (select calendar_date, calendar_date_id from {{ ref("src_dim_date") }}),

    dim_wbx_account as (
        select
            upper(source_system) as source_system,
            account_guid as account_guid,
            source_account_identifier as source_account_identifier,
            tagetik_account as tagetik_account,
            source_company_code as source_company_code,
            source_business_unit_code as source_business_unit_code,
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

    conv_rt as (
        select
        /*
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
            */

            /*  Mike Traub - account_guids were not generating correctly.  This fix put in place on 7/13/2023.    */
            dim_wbx_account.account_guid as account_guid,
            dim_wbx_account.account_guid as discount_account_guid,
            dim_wbx_account.account_guid as writeoff_account_guid,
            dim_wbx_account.account_guid as chargeback_account_guid,
            dim_wbx_account.account_guid as deduction_account_guid,
            /*
            {{
                dbt_utils.surrogate_key(
                    ["src.source_system", "source_concat_nat_key"]
                )
            }} as discount_account_guid,
            {{
                dbt_utils.surrogate_key(
                    ["src.source_system", "source_concat_nat_key"]
                )
            }} as writeoff_account_guid,
            {{
                dbt_utils.surrogate_key(
                    ["src.source_system", "source_concat_nat_key"]
                )
            }} as chargeback_account_guid,
            {{
                dbt_utils.surrogate_key(
                    ["src.source_system", "source_concat_nat_key"]
                )
            }} as deduction_account_guid,
            */
            {{
                dbt_utils.surrogate_key(
                    [
                        "src.source_system",
                        "src.source_customer_identifier",
                        "'CUSTOMER_MAIN'",
                    ]
                )
            }} as customer_address_number_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "src.source_system",
                        "src.source_payor_identifier",
                        "'CUSTOMER_MAIN'",
                    ]
                )
            }} as payor_address_number_guid,
            src.writeoff_src_acct_id as writeoff_src_accnt_identifier,
            src.chargeback_src_acct_id as chargeback_src_accnt_identifier,
            src.deduction_src_acct_id as deduction_src_accnt_identifier,
            src.discount_src_acct_id as discount_src_accnt_identifier,
            writeoff.tagetik_account as writeoff_trgt_accnt_identifier,
            chargebk.tagetik_account as chargeback_trgt_accnt_identifier,
            deduction.tagetik_account as deduction_trgt_accnt_identifier,
            discount.tagetik_account as discount_trgt_accnt_identifier,
            src.writeoff_reason_code,
            src.chargeback_reason_code,
            src.deduction_reason_code,
            src.void_reason_code,
            src.source_system as source_system,
            src.payment_identifier,
            cast(src.line_number as number(38, 10)) as line_number,
            src.receipt_number,
            src.document_number,
            src.document_type as source_document_type,
            nvl(
                to_char(document_type_lkp.normalized_value), to_char(src.document_type)
            ) as document_type,
            src.document_company,
            src.document_pay_item,
            src.source_customer_identifier,
            src.gl_date,
            src.gl_posted_flag,
            src.batch_type,
            src.batch_number,
            src.batch_date,
            src.foreign_transaction_flag,
            src.company_code,
            src.source_account_identifier,
            dim_wbx_account.consolidation_account as target_account_identifier,
            src.receipt_type_code,
            src.source_payor_identifier,
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
            src.net_due_date,
            src.deduction_document_number,
            src.deduction_document_type,
            src.deduction_document_company,
            src.deduction_document_pay_item,
            src.journal_document_type,
            src.journal_document_number,
            src.journal_document_company,
            src.reference_txt,
            src.remark_txt,
            src.source_payment_instr_code,
            nvl(
                to_char(source_payment_instr_desc_lkp.normalized_value),
                to_char(src.source_payment_instr_code)
            ) as payment_instr_desc,
            nvl(
                to_char(source_payment_instr_code_lkp.normalized_value),
                to_char(src.source_payment_instr_code)
            ) as target_payment_instr_code,
            src.void_date,
            src.discount_date,
            src.inv_jrnl_date,
            nvl(
                to_char(txn_currency_lkp.normalized_value), to_char(src.txn_currency)
            ) as transaction_currency,
            'USD' as phi_currency,
            ref_effective_currency_dim.company_default_currency_code as base_currency,
            ref_effective_currency_dim.parent_currency_code as pcomp_currency,
            '1' as base_conv_rt,
            case
                when
                    ref_effective_currency_dim.company_default_currency_code = nvl(
                        to_char(txn_currency_lkp.normalized_value),
                        to_char(src.txn_currency)
                    )
                then 1
                else txn_conv_rt_lkp.curr_conv_rt
            end as txn_conv_rt,
            case
                when ref_effective_currency_dim.company_default_currency_code = 'USD'
                then 1
                else phi_conv_rt_lkp.curr_conv_rt
            end as phi_conv_rt,
            case
                when
                    ref_effective_currency_dim.company_default_currency_code
                    = ref_effective_currency_dim.parent_currency_code
                then 1
                else pcomp_conv_rt_lkp.curr_conv_rt
            end as pcomp_conv_rt,

            src.txn_payment_amt,
            src.txn_discount_avail_amt,
            src.txn_discount_taken_amt,
            src.txn_writeoff_amt,
            src.txn_chargeback_amt,
            src.txn_deduction_amt,
            src.txn_gain_loss_amt,
            src.base_payment_amt,
            src.base_discount_avail_amt,
            src.base_discount_taken_amt,
            src.base_writeoff_amt,
            src.base_chargeback_amt,
            src.base_deduction_amt,
            src.base_gain_loss_amt,
            src.intercompany_flag,
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
            and dim_wbx_account.source_business_unit_code
            = src.source_business_unit_code

        left join
            dim_wbx_account writeoff
            on writeoff.source_system = src.source_system
            and writeoff.source_account_identifier = src.writeoff_src_acct_id
            and writeoff.source_company_code = src.company_code
            and writeoff.source_business_unit_code = src.source_business_unit_code

        left join
            dim_wbx_account chargebk
            on chargebk.source_system = src.source_system
            and chargebk.source_account_identifier = src.chargeback_src_acct_id
            and chargebk.source_company_code = src.company_code
            and chargebk.source_business_unit_code = src.source_business_unit_code

        left join
            dim_wbx_account deduction
            on deduction.source_system = src.source_system
            and deduction.source_account_identifier = src.deduction_src_acct_id
            and deduction.source_company_code = src.company_code
            and deduction.source_business_unit_code = src.source_business_unit_code

        left join
            dim_wbx_account discount
            on discount.source_system = src.source_system
            and discount.source_account_identifier = src.discount_src_acct_id
            and discount.source_company_code = src.company_code
            and discount.source_business_unit_code = src.source_business_unit_code

        left join
            ref_effective_currency_dim
            on ref_effective_currency_dim.source_system = src.source_system
            and ref_effective_currency_dim.company_code = src.company_code
            and ref_effective_currency_dim.effective_date <= src.gl_date
            and ref_effective_currency_dim.expiration_date >= src.gl_date

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
                    "ADDRESS_BOOK",
                    "PAYMENT_TYPE_CODE",
                    "UPPER(SRC.source_payment_instr_code)",
                    "source_payment_instr_code_LKP",
                )
            }}
        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "ADDRESS_BOOK",
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
                    "CUST_CURRENCY_CODE",
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
                    "SRC.gl_date",
                    "txn_conv_rt_LKP",
                )
            }}

        left join
            {{
                lkp_exchange_rate_daily_oc(
                    "SRC.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "'USD'",
                    "SRC.gl_date",
                    "phi_conv_rt_LKP",
                )
            }}

        left join
            {{
                lkp_exchange_rate_daily_oc(
                    "SRC.SOURCE_SYSTEM",
                    "ref_effective_currency_dim.company_default_currency_code",
                    "ref_effective_currency_dim.parent_currency_code",
                    "SRC.gl_date",
                    "pcomp_conv_rt_LKP",
                )
            }}
    )

select

    cast(substring(source_system, 1, 255) as text(255)) as source_system,

    cast(substring(payment_identifier, 1, 255) as text(255)) as payment_identifier,

    cast(line_number as number(38, 10)) as line_number,

    cast(substring(receipt_number, 1, 255) as text(255)) as receipt_number,

    cast(substring(document_number, 1, 255) as text(255)) as document_number,

    cast(substring(document_type, 1, 255) as text(255)) as document_type,

    cast(substring(document_company, 1, 255) as text(255)) as document_company,

    cast(substring(document_pay_item, 1, 255) as text(255)) as document_pay_item,

    cast(gl_date as timestamp_ntz(9)) as gl_date,

    cast(substring(gl_posted_flag, 1, 6) as text(6)) as posted_flag,

    cast(
        substring(source_account_identifier, 1, 255) as text(255)
    ) as source_account_identifier,

    cast(
        substring(target_account_identifier, 1, 255) as text(255)
    ) as target_account_identifier,

    cast(account_guid as text(255)) as account_guid,

    cast(substring(company_code, 1, 255) as text(255)) as company_code,

    cast(
        substring(foreign_transaction_flag, 1, 20) as text(20)
    ) as foreign_transaction_flag,

    cast(
        substring(discount_src_accnt_identifier, 1, 255) as text(255)
    ) as discount_src_accnt_identifier,

    cast(
        substring(discount_trgt_accnt_identifier, 1, 255) as text(255)
    ) as discount_trgt_accnt_identifier,

    cast(discount_account_guid as text(255)) as discount_account_guid,

    cast(substring(writeoff_reason_code, 1, 255) as text(255)) as writeoff_reason_code,

    cast(
        substring(writeoff_src_accnt_identifier, 1, 255) as text(255)
    ) as writeoff_src_accnt_identifier,

    cast(
        substring(writeoff_trgt_accnt_identifier, 1, 255) as text(255)
    ) as writeoff_trgt_accnt_identifier,

    cast(writeoff_account_guid as text(255)) as writeoff_account_guid,

    cast(
        substring(chargeback_reason_code, 1, 255) as text(255)
    ) as chargeback_reason_code,

    cast(
        substring(chargeback_src_accnt_identifier, 1, 255) as text(255)
    ) as chargeback_src_accnt_idntifier,

    cast(
        substring(chargeback_trgt_accnt_identifier, 1, 255) as text(255)
    ) as chargeback_trgt_accnt_idntifer,

    cast(chargeback_account_guid as text(255)) as chargeback_account_guid,

    cast(
        substring(deduction_reason_code, 1, 255) as text(255)
    ) as deduction_reason_code,

    cast(
        substring(deduction_src_accnt_identifier, 1, 255) as text(255)
    ) as deduction_src_accnt_idntifer,

    cast(
        substring(deduction_trgt_accnt_identifier, 1, 255) as text(255)
    ) as deduction_trgt_accnt_idntifer,

    cast(deduction_account_guid as text(255)) as deduction_account_guid,

    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,

    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,

    cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,

    cast(
        substring(deduction_document_number, 1, 255) as text(255)
    ) as deduction_document_number,

    cast(
        substring(deduction_document_type, 1, 255) as text(255)
    ) as deduction_document_type,

    cast(
        substring(deduction_document_company, 1, 255) as text(255)
    ) as deduction_document_company,

    cast(
        substring(deduction_document_pay_item, 1, 255) as text(255)
    ) as deduction_document_pay_item,

    cast(
        substring(journal_document_number, 1, 255) as text(255)
    ) as journal_document_number,

    cast(
        substring(journal_document_type, 1, 255) as text(255)
    ) as journal_document_type,

    cast(
        substring(journal_document_company, 1, 255) as text(255)
    ) as journal_document_company,

    cast(void_date as timestamp_ntz(9)) as void_date,

    cast(substring(void_reason_code, 1, 255) as text(255)) as void_reason_code,

    cast(net_due_date as timestamp_ntz(9)) as net_due_date,

    cast(discount_date as timestamp_ntz(9)) as discount_date,

    cast(inv_jrnl_date as timestamp_ntz(9)) as inv_jrnl_date,

    cast(substring(reference_txt, 1, 255) as text(255)) as reference_txt,

    cast(
        substring(source_customer_identifier, 1, 255) as text(255)
    ) as source_customer_identifier,

    cast(customer_address_number_guid as text(255)) as customer_address_number_guid,


    cast(
        substring(source_payor_identifier, 1, 255) as text(255)
    ) as source_payor_identifier,

    cast(payor_address_number_guid as text(255)) as payor_address_number_guid,

    cast(
        substring(source_payment_instr_code, 1, 255) as text(255)
    ) as source_payment_instr_code,

    cast(
        substring(target_payment_instr_code, 1, 255) as text(255)
    ) as target_payment_instr_code,

    cast(substring(payment_instr_desc, 1, 255) as text(255)) as payment_instr_desc,

    cast(substring(transaction_currency, 1, 255) as text(255)) as transaction_currency,

    cast(substring(base_currency, 1, 255) as text(255)) as base_currency,

    cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,

    cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,

    cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,

    cast(base_conv_rt as number(29, 9)) as base_conv_rt,

    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,

    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,

    cast(txn_payment_amt as number(38, 10)) as txn_payment_amt,

    cast(txn_discount_avail_amt as number(38, 10)) as txn_discount_avail_amt,

    cast(txn_discount_taken_amt as number(38, 10)) as txn_discount_taken_amt,

    cast(txn_writeoff_amt as number(38, 10)) as txn_writeoff_amt,

    cast(txn_chargeback_amt as number(38, 10)) as txn_chargeback_amt,

    cast(txn_deduction_amt as number(38, 10)) as txn_deduction_amt,

    cast(txn_gain_loss_amt as number(38, 10)) as txn_gain_loss_amt,

    cast(base_payment_amt as number(38, 10)) as base_payment_amt,

    cast(base_discount_avail_amt as number(38, 10)) as base_discount_avail_amt,

    cast(base_discount_taken_amt as number(38, 10)) as base_discount_taken_amt,

    cast(base_writeoff_amt as number(38, 10)) as base_writeoff_amt,

    cast(base_chargeback_amt as number(38, 10)) as base_chargeback_amt,

    cast(base_deduction_amt as number(38, 10)) as base_deduction_amt,

    cast(base_gain_loss_amt as number(38, 10)) as base_gain_loss_amt,

    cast(base_payment_amt * phi_conv_rt as number(38, 10)) as phi_payment_amt,

    cast(
        base_discount_avail_amt * phi_conv_rt as number(38, 10)
    ) as phi_discount_avail_amt,

    cast(
        base_discount_taken_amt * phi_conv_rt as number(38, 10)
    ) as phi_discount_taken_amt,

    cast(base_writeoff_amt * phi_conv_rt as number(38, 10)) as phi_writeoff_amt,

    cast(base_chargeback_amt * phi_conv_rt as number(38, 10)) as phi_chargeback_amt,

    cast(base_deduction_amt * phi_conv_rt as number(38, 10)) as phi_deduction_amt,

    cast(base_gain_loss_amt * phi_conv_rt as number(38, 10)) as phi_gain_loss_amt,

    cast(base_payment_amt * pcomp_conv_rt as number(38, 10)) as pcomp_payment_amt,

    cast(
        base_discount_avail_amt * pcomp_conv_rt as number(38, 10)
    ) as pcomp_discount_avail_amt,

    cast(
        base_discount_taken_amt * pcomp_conv_rt as number(38, 10)
    ) as pcomp_discount_taken_amt,

    cast(base_writeoff_amt * pcomp_conv_rt as number(38, 10)) as pcomp_writeoff_amt,

    cast(base_chargeback_amt * pcomp_conv_rt as number(38, 10)) as pcomp_chargeback_amt,

    cast(base_deduction_amt * pcomp_conv_rt as number(38, 10)) as pcomp_deduction_amt,

    cast(base_gain_loss_amt * pcomp_conv_rt as number(38, 10)) as pcomp_gain_loss_amt,

    cast(substring(intercompany_flag, 1, 1) as text(1)) as intercompany_flag,

    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,

    cast(load_date as timestamp_ntz(9)) as load_date,

    cast(update_date as timestamp_ntz(9)) as update_date,

    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,

    cast(substring(batch_type, 1, 4) as text(4)) as batch_type,

    cast(substring(batch_number, 1, 255) as text(255)) as batch_number,

    cast(batch_date as timestamp_ntz(9)) as batch_date,

    cast(
        {{
            dbt_utils.surrogate_key(
                [
                    "conv_rt.source_system",
                    "conv_rt.deduction_document_number",
                    "conv_rt.deduction_document_type",
                    "conv_rt.deduction_document_company",
                    "conv_rt.deduction_document_pay_item",
                ]
            )
        }} as text(255)
    ) as deduction_document_guid,

    cast(
        {{
            dbt_utils.surrogate_key(
                [
                    "conv_rt.source_system",
                    "conv_rt.document_number",
                    "conv_rt.document_type",
                    "conv_rt.document_company",
                ]
            )
        }} as text(255)
    ) as ar_custinv_hdr_unique_key,
    cast(
        {{
            dbt_utils.surrogate_key(
                [
                    "conv_rt.source_system",
                    "conv_rt.payment_identifier",
                    "conv_rt.line_number",
                ]
            )
        }} as text(255)
    ) as unique_key

from conv_rt
