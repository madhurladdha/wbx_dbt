{{ config(tags=["finance", "po"]) }}

with
    src as (select * from {{ ref("stg_f_wbx_fin_prc_po") }}),

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

    source_transform as (
        select
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
            dim_wbx_account.tagetik_account as target_account_identifier,
            {{
                dbt_utils.surrogate_key(
                    [
                        "src.source_system",
                        "src.source_business_unit_code",
                        "'PLANT_DC'",
                    ]
                )
            }} as business_unit_address_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "src.source_system",
                        "src.source_supplier_identifier",
                        "'SUPPLIER'",
                        "src.po_order_company",
                    ]
                )
            }} as supplier_address_number_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "src.source_system",
                        "src.source_buyer_identifier",
                        "'BUSINESS_REP'",
                    ]
                )
            }} as buyer_address_number_guid,
            {{
                dbt_utils.surrogate_key(
                    [
                        "src.source_system",
                        "src.source_item_identifier",
                    ]
                )
            }} as item_guid,
            {{
                dbt_utils.surrogate_key(
                    ["src.source_system", "src.source_payment_terms_code"]
                )
            }} as payment_terms_guid,
            po_order_company,
            po_order_number,
            nvl(
                po_order_type_lkp.normalized_value, trim(src.po_order_type)
            ) as po_order_type,
            src.po_order_type as source_po_order_type,
            po_order_suffix,
            po_line_number,
            trim(src.source_business_unit_code) as source_business_unit_code,
            src.source_account_identifier,
            source_subledger_identifier,
            nvl(source_subledger_type, '_') as source_subledger_type,
            source_item_identifier,
            source_supplier_identifier,
            source_buyer_identifier,
            case when source_item_identifier is null then 'N' else 'S' end v_line_type,
            nvl(line_status_lkp.normalized_value, trim(src.line_status)) as line_status,
            line_unit_cost,
            line_on_hold_amt,
            line_open_amt,
            line_received_amt,
            line_onhold_quantity,
            line_open_quantity,
            line_order_quantity,
            line_recvd_quantity,
            po_gl_date,
            po_order_date,
            po_delivery_date,
            po_promised_delivery_date,
            po_requested_date,
            po_cancelled_date,
            contract_company_code,
            contract_number,
            nvl(
                contract_type_lkp.normalized_value, trim(src.contract_type)
            ) as contract_type,
            '-' as source_contract_type,
            to_number(contract_line_number) contract_line_number,
            agrmnt_number,
            agrmnt_suplmnt_number,
            case
                when contract_number is not null
                then 'C'
                else case when agrmnt_number is not null then 'A' else '' end
            end as contract_agreement_flag,
            po_line_desc,
            nvl(
                to_char(txn_uom_lkp.normalized_value),
                trim(upper(to_char(src.transaction_uom)))
            ) as transaction_uom,
            trim(source_payment_terms_code) source_payment_terms_code,
            source_freight_handling_code,
            nvl(
                freight_handling_code_lkp.normalized_value,
                trim(src.source_freight_handling_code)
            ) as target_freight_handling_code,
            nvl(
                freight_handling_desc_lkp.normalized_value,
                trim(src.source_freight_handling_code)
            ) as freight_handling_code_desc,
            gl_offset_srccd,
            nvl(
                to_char(txn_currency_lkp.normalized_value),
                to_char(src.transaction_currency)
            ) as transaction_currency,
            source_updated_datetime as source_date_updated,
            1 as source_updated_d_id,
            src.source_system as source_system,
            rma_number,
            agreement_number,
            source_cost_center,
            caf_no,
            project_id,
            project_name,
            project_category,
            source_object_code,
            line_total_amount as order_total_amount,
            null as po_org_promise_date,
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
            and dim_wbx_account.source_company_code = src.po_order_company
            and dim_wbx_account.source_business_unit_code = src.source_cost_center

        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "ADDRESS_BOOK",
                    "SUPP_CURRENCY_CODE",
                    "UPPER(SRC.transaction_currency)",
                    "txn_currency_LKP",
                )
            }}

        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "ITEM",
                    "PRIMARY_UOM",
                    "UPPER(SRC.transaction_uom)",
                    "txn_uom_LKP",
                )
            }}

        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "FINANCE",
                    "PO_DOC_TYPE_CODE",
                    "trim(src.po_order_type)",
                    "po_order_type_LKP",
                )
            }}

        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "FINANCE",
                    "DOCUMENT_TYPE_CODE",
                    "trim(src.contract_type)",
                    "contract_type_LKP",
                )
            }}

        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "FINANCE",
                    "LINE_STATUS_DESC",
                    "trim(src.line_status)",
                    "line_status_LKP",
                )
            }}

        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "ADDRESS_BOOK",
                    "FREIGHT_HANDLING_CODE",
                    "trim(upper(src.source_freight_handling_code))",
                    "freight_handling_code_lkp",
                )
            }}

        left join
            {{
                lkp_normalization(
                    "SRC.SOURCE_SYSTEM",
                    "ADDRESS_BOOK",
                    "FREIGHT_HANDLING_DESC",
                    "trim(upper(src.source_freight_handling_code))",
                    "freight_handling_desc_lkp",
                )
            }}
    ),

    guid_gen as (
        select
            source_transform.*,
            nvl(
                line_type_lkp.normalized_value, trim(source_transform.v_line_type)
            ) as line_type,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source_transform.source_system",
                        "po_order_company",
                        "po_order_number",
                        "po_order_type",
                        "po_order_suffix",
                        "po_line_number",
                    ]
                )
            }} as unique_key,
            {{
                dbt_utils.surrogate_key(
                    [
                        "contract_company_code",
                        "contract_number",
                        "contract_type",
                        "contract_line_number",
                    ]
                )
            }} as contract_agreement_guid
        from source_transform
        left join
            {{
                lkp_normalization(
                    "source_transform.SOURCE_SYSTEM",
                    "FINANCE",
                    "LINE_TYPE_CODE",
                    "trim(source_transform.v_line_type)",
                    "line_type_LKP",
                )
            }}

    ),

    exp_conv as (
        select
            guid_gen.*,
            add_months(guid_gen.po_order_date, -1) as prev_mnth,
            last_day(prev_mnth) as last_day_of_prev_month,
            adr_company_master_dim_po_order_date.default_currency_code as base_currency,
            'USD' as phi_currency,
            adr_company_master_dim_po_order_date.parent_currency_code as pcomp_currency
        from guid_gen
        left join
            dim_date as dim_date_v_po_order_date
            on dim_date_v_po_order_date.calendar_date = guid_gen.po_order_date
        left join
            dim_company as adr_company_master_dim_po_order_date
            on adr_company_master_dim_po_order_date.source_system
            = guid_gen.source_system
            and adr_company_master_dim_po_order_date.company_code
            = guid_gen.po_order_company
            and adr_company_master_dim_po_order_date.effective_date <= po_order_date
            and adr_company_master_dim_po_order_date.expiration_date >= po_order_date
    ),

    exp_exch_rt_calc as (
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
                when txn_conv_rt <> 0 then order_total_amount / txn_conv_rt else 0
            end as base_order_total_amount,
            case
                when txn_conv_rt <> 0 then line_on_hold_amt / txn_conv_rt else 0
            end as base_line_on_hold_amt,
            case
                when txn_conv_rt <> 0 then line_open_amt / txn_conv_rt else 0
            end as base_line_open_amt,
            case
                when txn_conv_rt <> 0 then line_received_amt / txn_conv_rt else 0
            end as base_line_received_amt,
            case
                when txn_conv_rt <> 0 then line_unit_cost / txn_conv_rt else 0
            end as base_line_unit_cost,
            base_order_total_amount * txn_conv_rt as txn_order_total_amount,
            base_line_on_hold_amt * txn_conv_rt as txn_line_on_hold_amt,
            base_line_open_amt * txn_conv_rt as txn_line_open_amt,
            base_line_received_amt * txn_conv_rt as txn_line_received_amt,
            base_line_unit_cost * txn_conv_rt as txn_line_unit_cost,
            base_order_total_amount * phi_conv_rt as phi_order_total_amount,
            base_line_on_hold_amt * phi_conv_rt as phi_line_on_hold_amt,
            base_line_open_amt * phi_conv_rt as phi_line_open_amt,
            base_line_received_amt * phi_conv_rt as phi_line_received_amt,
            base_line_unit_cost * phi_conv_rt as phi_line_unit_cost,
            base_order_total_amount * pcomp_conv_rt as pcomp_order_total_amount,
            base_line_on_hold_amt * pcomp_conv_rt as pcomp_line_on_hold_amt,
            base_line_open_amt * pcomp_conv_rt as pcomp_line_open_amt,
            base_line_received_amt * pcomp_conv_rt as pcomp_line_received_amt,
            base_line_unit_cost * pcomp_conv_rt as pcomp_line_unit_cost
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

    final as (

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

            cast(substring(transaction_currency, 1, 10) as text(10)) as txn_currency,

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

        from exp_exch_rt_calc
    )

select *
from final
