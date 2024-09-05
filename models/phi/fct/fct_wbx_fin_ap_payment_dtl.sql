{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["finance", "ap"],
        transient=false,
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns",
        pre_hook="""
            {% set now = modules.datetime.datetime.now() %}
            {%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
            {%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}
            {% if day_today == full_load_day %}
            {{ truncate_if_exists(this.schema, this.table) }}
            {% endif %}
            """,
    )
}}

with
/* Captures the main D365 transactional data set. */
src as (
    select
        *,
        row_number() over (partition by unique_key order by 1) as rownum
    from {{ ref("int_f_wbx_fin_ap_payment_dtl") }}
),
/* Captures the main AX history transactional data set.  This model already has any dim values converted as intended. */
ax_hist as (select * from {{ ref('conv_fct_wbx_fin_ap_payment_dtl') }}),

fact as (
    select
        source_system,
        payment_identifier,
        line_number,
        document_company,
        document_type,
        document_number,
        document_pay_item,
        document_pay_item_ext,
        source_business_unit_code,
        business_unit_address_guid,
        source_payee_identifier,
        payee_address_number_guid,
        source_account_identifier,
        target_account_identifier,
        account_guid,
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
        unique_key,
        'D365' as source_legacy
    from src
    where rownum = 1
),

old_ax_hist as (
    select
        a.source_system,
        a.payment_identifier,
        a.line_number,
        a.document_company,
        a.document_type,
        a.document_number,
        a.document_pay_item,
        a.document_pay_item_ext,
        a.source_business_unit_code,
        a.business_unit_address_guid,
        a.source_payee_identifier,
        a.payee_address_number_guid,
        a.source_account_identifier,
        a.target_account_identifier,
        a.account_guid,
        a.source_payment_instr_code,
        a.target_payment_instr_code,
        a.payment_instr_desc,
        a.gl_posted_flag,
        a.purchase_order_number,
        a.remark_txt,
        a.transaction_currency,
        a.base_currency,
        a.pcomp_currency,
        a.phi_currency,
        a.txn_conv_rt,
        a.base_conv_rt,
        a.phi_conv_rt,
        a.pcomp_conv_rt,
        a.txn_payment_amt,
        a.txn_discount_available_amt,
        a.txn_discount_taken_amt,
        a.base_payment_amt,
        a.base_discount_available_amt,
        a.base_discount_taken_amt,
        a.pcomp_payment_amt,
        a.pcomp_discount_available_amt,
        a.pcomp_discount_taken_amt,
        a.phi_payment_amt,
        a.phi_discount_available_amt,
        a.phi_discount_taken_amt,
        a.payment_trans_doc_number,
        a.payment_trans_date,
        a.void_date,
        a.payment_trans_cleared_date,
        a.void_flag,
        a.source_date_updated,
        a.load_date,
        a.update_date,
        a.source_updated_d_id,
        a.ap_voucher_hdr_unique_key,
        a.unique_key,
       'AX' as source_legacy
    from ax_hist as a
    left join fact as b on a.unique_key = b.unique_key
    where b.source_system is null
),
/* Combines (union) the D365 data w/ the AX History set.    */
final as (
    select * from fact
    union all
    select * from old_ax_hist
)

select * from final
qualify row_number() over (partition by unique_key order by unique_key desc) = 1
